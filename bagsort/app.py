"""Assembly of the handling line.

Every subsystem is constructed here and every operator visible action is a
method on :class:`Plant`, which is what keeps the console handlers thin and
lets the CLI drive the same operations the HTTP API exposes.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass

from bagsort.audit.chain import AuditChain
from bagsort.batch.attributor import BatchAttributor
from bagsort.batch.history import HistoryVerification, verify_history
from bagsort.batch.ledger import BatchLedger
from bagsort.batch import policy
from bagsort.belt.belt import Belt
from bagsort.checkin.desk import Desk
from bagsort.clock import Clock
from bagsort.errors import ConflictError
from bagsort.events.bus import EventBus
from bagsort.flight.book import FlightBook
from bagsort.flight.close import FlightCloser
from bagsort.flight.mapping import BagMapping
from bagsort.flight.schedule import ScheduleBoard
from bagsort.ids import IdFactory
from bagsort.inject.injector import Injector
from bagsort.model.entities import Bag, Flight, Reading, SortRecord
from bagsort.model.errors import unknown_bag
from bagsort.ns.chutes import ChuteRegistry
from bagsort.ns.namespace import Namespace
from bagsort.ns.topology import handling_line
from bagsort.quota.quota import QuotaBook
from bagsort.recheck.queue import RecheckQueue
from bagsort.reporting.report import PlantReport
from bagsort.sequence.verdict import VerdictTracker
from bagsort.sorter.sorter import Sorter
from bagsort.store.store import Store
from bagsort.tag.history import ReadHistory
from bagsort.tag.reader import TagReader
from bagsort.chute.assigner import ChuteAssigner
from bagsort.chute.history import AssignmentHistory
from bagsort.console.metrics import Metrics
from bagsort.console.telemetry import Telemetry

DEFAULT_TERMINAL = "T1"


@dataclass
class Plant:
    """The wired plant plus the operator level operations the console calls."""

    store: Store
    clock: Clock
    namespace: Namespace
    registry: ChuteRegistry
    bus: EventBus
    telemetry: Telemetry
    metrics: Metrics
    reader: TagReader
    reads: ReadHistory
    book: FlightBook
    mapping: BagMapping
    schedule: ScheduleBoard
    closer: FlightCloser
    chutes: ChuteAssigner
    chute_history: AssignmentHistory
    sorter: Sorter
    injector: Injector
    desk: Desk
    recheck: RecheckQueue
    belt: Belt
    quota: QuotaBook
    audit: AuditChain
    batches: BatchLedger
    report: PlantReport

    @property
    def topology(self):
        return handling_line()

    # -- construction ------------------------------------------------------

    @classmethod
    def build(
        cls,
        store: Store,
        terminal: str = DEFAULT_TERMINAL,
        clock: Clock | None = None,
    ) -> "Plant":
        clock = clock or Clock()
        namespace = Namespace(terminal)
        registry = ChuteRegistry()
        bus = EventBus()
        telemetry = Telemetry()
        metrics = Metrics()
        bus.subscribe("*", lambda event: telemetry.observe(event.topic))

        reader = TagReader(store, namespace, clock)
        reads = ReadHistory(store)
        book = FlightBook(store, clock)
        mapping = BagMapping(store, clock)
        schedule = ScheduleBoard(store, clock)
        chutes = ChuteAssigner(store, clock)
        chute_history = AssignmentHistory(store)
        batches = BatchLedger(store, clock)
        attributor = BatchAttributor(batches, handoff=None)
        sorter = Sorter(
            store, mapping, chutes, clock, VerdictTracker(), attributor=attributor
        )
        injector = Injector(store, book, sorter, clock)
        quota = QuotaBook(store, clock)
        desk = Desk(store, reader, book, mapping, injector, quota, clock, handling_line())
        closer = FlightCloser(book)
        recheck = RecheckQueue(store, book, clock)
        belt = Belt(store, clock, IdFactory("cmd"))
        # The handoff closes the carousel into load records inside the batch
        # commit, and dispatch late allocations use the flight's carousel.
        attributor.bind_handoff(belt)

        audit = AuditChain(store, clock, handling_line())
        audit_subscriber = _AuditSubscriber(audit)
        for topic in (
            "bag.checked_in",
            "bag.sorted",
            "bag.diverted",
            "bag.recheck_enqueued",
            "flight.closed",
            "chute.assigned",
            "belt.switched",
            "batch.closed",
            "bag.remapped",
        ):
            bus.subscribe(topic, audit_subscriber)

        report = PlantReport(
            store, book, chutes, belt, quota, audit, namespace, handling_line(), batches
        )
        return cls(
            store=store,
            clock=clock,
            namespace=namespace,
            registry=registry,
            bus=bus,
            telemetry=telemetry,
            metrics=metrics,
            reader=reader,
            reads=reads,
            book=book,
            mapping=mapping,
            schedule=schedule,
            closer=closer,
            chutes=chutes,
            chute_history=chute_history,
            sorter=sorter,
            injector=injector,
            desk=desk,
            recheck=recheck,
            belt=belt,
            quota=quota,
            audit=audit,
            batches=batches,
            report=report,
        )

    # -- operator operations ----------------------------------------------

    def register_flight(self, code: str, carousel: str = "", chute: str = "") -> Flight:
        flight = Flight(
            id=self.namespace.flight_id(code),
            code=code.strip().upper(),
            terminal=self.namespace.terminal,
            carousel=self.namespace.carousel_id(carousel) if carousel else "",
        )
        self.book.register(flight)
        if chute:
            self.assign_chute(flight.id, self.namespace.chute_id(chute))
        return self.book.get(flight.id)

    def assign_chute(self, flight_id: str, chute_id: str):
        assignment = self.chutes.assign(flight_id, chute_id)
        self.bus.publish("chute.assigned", flight_id=flight_id, chute_id=chute_id)
        return assignment

    def read_barcode(self, barcode: str) -> Reading:
        return self.reader.read(barcode)

    def check_in(self, barcode: str, flight_id: str) -> Bag:
        bag = self.desk.check_in(barcode, flight_id)
        self.bus.publish("bag.checked_in", bag_id=bag.id, flight_id=flight_id)
        return bag

    def cancel_check_in(self, bag_id: str) -> Bag:
        bag = self.desk.cancel(bag_id)
        self.bus.publish("bag.diverted", bag_id=bag.id, reason="checkin_cancelled")
        return bag

    def bag(self, bag_id: str) -> Bag:
        bag = self.store.state().bags.get(bag_id)
        if bag is None:
            raise unknown_bag(bag_id)
        return bag

    def push_bag(self, bag_id: str, chute_id: str) -> SortRecord:
        bag = self.bag(bag_id)
        with self._boundary_commit() as announce:
            result = self.injector.push(bag, chute_id)
        self.bus.publish("bag.sorted", bag_id=bag.id, chute_id=result.chute_id)
        announce()
        return result

    def retry_bag(self, bag_id: str) -> bool:
        bag = self.bag(bag_id)
        with self._boundary_commit() as announce:
            sorted_now = self.injector.retry(bag)
        if sorted_now:
            self.bus.publish("bag.sorted", bag_id=bag.id, retry=True)
        announce()
        return sorted_now

    def _closed_batch_ids(self) -> set[str]:
        from bagsort.model.enums import BatchState

        return {
            batch.id
            for batch in self.store.state().batches.values()
            if batch.state is BatchState.CLOSED
        }

    @contextmanager
    def _boundary_commit(self):
        """Capture batch boundaries an operation seals and flush them on exit.

        Any dispatch can roll a due batch over, so every dispatch entry point
        uses this: after the store commit it publishes one ``batch.closed``
        event per newly sealed batch and persists the non-revertible commit
        point. A dispatch that sealed nothing persists nothing extra.
        """

        closed_before = self._closed_batch_ids()
        yield lambda: self._announce_newly_closed(closed_before)

    def _announce_newly_closed(self, closed_before: set[str]) -> None:
        for batch_id in sorted(self._closed_batch_ids() - closed_before):
            self._announce_closed(batch_id)

    def _announce_closed(self, batch_id: str) -> None:
        batch = self.batches.get(batch_id)
        self.bus.publish(
            "batch.closed",
            flight_id=batch.flight_id,
            batch_id=batch.id,
            number=batch.number,
        )
        # A boundary is a non-revertible commit point: flush it to disk.
        self.store.persist()

    def divert_bag(self, bag_id: str) -> Bag:
        bag = self.bag(bag_id)
        updated = self.sorter.divert(bag)
        self.bus.publish("bag.diverted", bag_id=bag.id)
        return updated

    def remap_bag(self, bag_id: str, flight_id: str) -> Bag:
        self.book.require_open(flight_id)
        self.mapping.remap(bag_id, flight_id)
        self.bus.publish("bag.remapped", bag_id=bag_id, flight_id=flight_id)
        return self.bag(bag_id)

    def revise_schedule(self, flight_id: str, cutoff_at: str):
        return self.schedule.revise(flight_id, cutoff_at)

    def close_flight(self, flight_id: str) -> Flight:
        flight = self.closer.close(flight_id, self.injector)
        self.bus.publish("flight.closed", flight_id=flight_id)
        return flight

    def enqueue_recheck(self, bag_id: str) -> Bag:
        bag = self.bag(bag_id)
        self.recheck.enqueue(bag)
        self.bus.publish("bag.recheck_enqueued", bag_id=bag_id)
        return bag

    def process_recheck(self) -> Bag | None:
        return self.recheck.dequeue()

    def switch_belt(self, carousel: str, batch: int) -> int:
        cleared = self.belt.switch(carousel, batch)
        self.bus.publish("belt.switched", carousel=carousel, batch=batch)
        return cleared

    def rotate_belt(self) -> int:
        return self.belt.rotate()

    def append_transfer(self, flight_id: str, carousel: str, batch: int):
        return self.belt.append_transfer(flight_id, carousel, batch)

    def set_quota(self, flight_id: str, limit: int) -> int:
        return self.quota.set_limit(flight_id, limit)

    # -- sorting batches ---------------------------------------------------

    def arm_batch(
        self,
        flight_id: str,
        carousel: str = "",
        window_seconds: int = policy.DEFAULT_WINDOW_SECONDS,
    ):
        """Open (or idempotently return) the current sorting batch."""

        batch = self.batches.arm(flight_id, carousel, window_seconds)
        # First batch of a carousel: announce it so downstream sees the pointer.
        if not self.belt.carousel() and batch.carousel:
            self.belt.set(batch.carousel, batch.number)
        return batch

    def close_batch(self, batch_id: str):
        """Close one due batch, hand the carousel over, and commit to disk."""

        commit_before = self.batches.get(batch_id).commit_seq
        batch = self.batches.close_batch(batch_id, self.belt)
        if batch.commit_seq != commit_before:
            self.bus.publish(
                "batch.closed",
                flight_id=batch.flight_id,
                batch_id=batch.id,
                number=batch.number,
            )
            self.store.persist()
        return batch

    def tick_batches(self) -> list:
        """Close every batch whose countdown has elapsed; idempotent."""

        before = self._closed_batch_ids()
        closed = self.batches.tick(self.belt)
        for batch in closed:
            if batch.id not in before:
                self.bus.publish(
                    "batch.closed",
                    flight_id=batch.flight_id,
                    batch_id=batch.id,
                    number=batch.number,
                )
        if closed:
            self.store.persist()
        return closed

    def batch_history(self, flight_id: str | None = None) -> list:
        if flight_id is None:
            return self.batches.history()
        return [
            item
            for item in self.batches.history()
            if item.flight_id == flight_id
        ]

    def bag_batch(self, bag_id: str) -> dict[str, object] | None:
        """Which sealed/open batch a bag was booked into, if any."""

        item = self.batches.item_for_bag(bag_id)
        if item is None:
            return None
        batch = self.batches.get(item.batch_id)
        return {"item": item, "batch": batch}

    def verify_batches(self) -> HistoryVerification:
        return verify_history(self.store.state())

    def batch_policy(self) -> dict[str, str]:
        return policy.policy_description()

    def verify(self) -> bool:
        return self.audit.verify()


class _AuditSubscriber:
    """Turns published operations into linked audit entries."""

    _STAGE = {
        "bag.checked_in": ("checkin", "checkin"),
        "bag.sorted": ("sort", "sorter"),
        "bag.diverted": ("diversion", "sorter"),
        "bag.recheck_enqueued": ("recheck", "recheck"),
        "flight.closed": ("flight_close", "audit"),
        "chute.assigned": ("chute_assign", "chute"),
        "belt.switched": ("belt_switch", "belt"),
        "batch.closed": ("batch_close", "belt"),
        "bag.remapped": ("remap", "checkin"),
    }

    def __init__(self, audit: AuditChain) -> None:
        self._audit = audit

    def __call__(self, event) -> None:
        mapping = self._STAGE.get(event.topic)
        if mapping is None:
            raise ConflictError(f"no audit mapping for topic {event.topic!r}")
        kind, stage = mapping
        subject = str(
            event.payload.get("bag_id")
            or event.payload.get("flight_id")
            or event.payload.get("carousel")
            or ""
        )
        detail = {
            key: value
            for key, value in event.payload.items()
            if key not in {"bag_id", "flight_id"}
        }
        self._audit.record(kind, subject, stage, detail)
