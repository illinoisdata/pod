from __future__ import annotations  # isort:skip

from bisect import bisect_right
from typing import Dict, List, Optional, Set, Tuple

from pod.common import Object, ObjectId, PodId

MemoId = int


class MemoPageAllocator:
    VIRTUAL_OFFSET = 2**31  # [VIRTUAL_OFFSET, 2 ** 32) virtually points to global memo indices.
    PAGE_SIZE = 2**10  # Number of memo objects per page.

    def __init__(self) -> None:
        self.oid_pages: Dict[ObjectId, List[MemoId]] = {}
        self.oid_latest_idxs: Dict[ObjectId, int] = {}
        self.latest_page_offset: MemoId = 0

    def allocate(self, pid: PodId) -> MemoId:
        oid = pid.oid
        if oid not in self.oid_latest_idxs:
            self.oid_pages[oid] = []
            self.oid_latest_idxs[oid] = 0

        # Allocate new page if needed.
        while self.oid_latest_idxs[oid] >= len(self.oid_pages[oid]):
            page_offset = self._allocate()
            self.oid_pages[oid].append(page_offset)

        # Return and register new pid.
        page_offset = self.oid_pages[oid][self.oid_latest_idxs[oid]]
        self.oid_latest_idxs[oid] += 1
        return page_offset

    def reset(self):
        for oid in self.oid_latest_idxs:
            self.oid_latest_idxs[oid] = 0

    def clear(self):
        self.oid_pages = {}
        self.oid_latest_idxs = {}
        self.latest_page_offset = 0

    def _allocate(self) -> MemoId:
        page_offset = self.latest_page_offset
        self.latest_page_offset += MemoPageAllocator.PAGE_SIZE
        assert page_offset < MemoPageAllocator.VIRTUAL_OFFSET
        return page_offset


class StaticPodPicklerMemo:
    PAGE_ALLOCATOR = MemoPageAllocator()

    def __init__(self) -> None:
        self.physical_memo: Dict[ObjectId, Tuple[MemoId, Object]] = {}
        self.page_pid: Dict[MemoId, PodId] = {}
        StaticPodPicklerMemo.PAGE_ALLOCATOR.reset()

    def next_page_offset(self, pid: PodId) -> MemoId:  # Next ID.
        page_offset = StaticPodPicklerMemo.PAGE_ALLOCATOR.allocate(pid)
        self.page_pid[page_offset] = pid
        return page_offset

    def __setitem__(self, obj_id: ObjectId, val: Tuple[MemoId, Object]) -> None:
        self.physical_memo[obj_id] = val

    def __getitem__(self, obj_id: ObjectId) -> Tuple[MemoId, Object, PodId]:
        memo_id, obj = self.physical_memo[obj_id]
        return memo_id, obj, self.page_pid[memo_id - memo_id % MemoPageAllocator.PAGE_SIZE]

    def __contains__(self, obj_id: ObjectId) -> bool:
        return obj_id in self.physical_memo

    def clear(self) -> None:
        self.physical_memo.clear()
        self.page_pid.clear()
        self.latest_page_offset = 0

    def next_view(self, pid: PodId) -> StaticPodPicklerMemoView:
        return StaticPodPicklerMemoView(self, pid)


class StaticPodPicklerMemoView:
    def __init__(self, memo: StaticPodPicklerMemo, pid: PodId) -> None:
        self.memo: StaticPodPicklerMemo = memo
        self.pid = pid
        self.page_offsets: List[MemoId] = [self.memo.next_page_offset(self.pid)]
        self.next_id = 0
        self.dep_pids: List[PodId] = []

    def __len__(self) -> MemoId:  # Next ID
        return self.next_id

    def __setitem__(self, obj_id: ObjectId, val: Tuple[MemoId, Object]) -> None:
        assert val[0] == self.next_id
        page_idx = val[0] // MemoPageAllocator.PAGE_SIZE
        local_offset = val[0] % MemoPageAllocator.PAGE_SIZE
        if page_idx >= len(self.page_offsets):
            self.page_offsets.append(self.memo.next_page_offset(self.pid))
        self.memo[obj_id] = (local_offset + self.page_offsets[page_idx], val[1])
        self.next_id += 1

    def __getitem__(self, obj_id: ObjectId) -> Tuple[MemoId, Object]:
        memo_id, obj, dep_pid = self.memo[obj_id]
        page_idx = bisect_right(self.page_offsets, memo_id) - 1
        if page_idx >= 0 and memo_id < self.page_offsets[page_idx] + MemoPageAllocator.PAGE_SIZE:
            # Implicit: self.page_offsets[page_idx] <= memo_id.
            assert self.next_id > 0
            memo_id = (memo_id - self.page_offsets[page_idx]) + page_idx * MemoPageAllocator.PAGE_SIZE
        else:
            memo_id += MemoPageAllocator.VIRTUAL_OFFSET
            self.dep_pids.append(dep_pid)
        return (memo_id, obj)

    def __contains__(self, obj_id: ObjectId) -> bool:
        return obj_id in self.memo

    def get(self, obj_id: ObjectId, default: Object = None) -> Optional[Tuple[MemoId, Object]]:
        return self[obj_id] if obj_id in self.memo else default

    def get_dep_pids(self) -> Set[PodId]:
        return set(self.dep_pids)


class StaticPodUnpicklerMemo:
    def __init__(self) -> None:
        self.physical_memo: Dict[MemoId, Object] = {}

    def __setitem__(self, memo_id: MemoId, obj: Object) -> None:
        self.physical_memo[memo_id] = obj

    def __getitem__(self, memo_id: ObjectId) -> Object:
        return self.physical_memo[memo_id]

    def view(self, page_offsets: List[MemoId]) -> StaticPodUnpicklerMemoView:
        return StaticPodUnpicklerMemoView(self, page_offsets)


class StaticPodUnpicklerMemoView:
    def __init__(self, memo: StaticPodUnpicklerMemo, page_offsets: List[MemoId]) -> None:
        self.memo: StaticPodUnpicklerMemo = memo
        self.page_offsets = page_offsets
        self.next_id = 0

    def __len__(self) -> MemoId:  # Next ID
        return self.next_id

    def __setitem__(self, memo_id: MemoId, obj: Object) -> None:
        assert memo_id == self.next_id
        page_idx = memo_id // MemoPageAllocator.PAGE_SIZE
        memo_id = memo_id % MemoPageAllocator.PAGE_SIZE + self.page_offsets[page_idx]
        self.memo[memo_id] = obj
        self.next_id += 1

    def __getitem__(self, memo_id: MemoId) -> Object:
        if memo_id < MemoPageAllocator.VIRTUAL_OFFSET:
            page_idx = memo_id // MemoPageAllocator.PAGE_SIZE
            memo_id = memo_id % MemoPageAllocator.PAGE_SIZE + self.page_offsets[page_idx]
        else:
            memo_id -= MemoPageAllocator.VIRTUAL_OFFSET
        return self.memo[memo_id]
