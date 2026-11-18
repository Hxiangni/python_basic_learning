"""MyQueue 单元测试（pytest 版）：单线程行为 + 多线程并发行为"""
import threading
import time
from queue import Empty, Full

import pytest

from multithreading_queue import MyQueue


@pytest.fixture
def unbounded() -> MyQueue[int]:
    """无界队列（maxsize=0）"""
    return MyQueue()


@pytest.fixture
def bounded() -> MyQueue[int]:
    """容量为 2 的有界队列"""
    return MyQueue(maxsize=2)


class TestBasicBehavior:
    """单线程基础行为"""

    def test_fifo_order(self, unbounded: MyQueue[int]):
        """先进先出"""
        for i in range(5):
            unbounded.put(i)
        assert [unbounded.get() for _ in range(5)] == [0, 1, 2, 3, 4]

    def test_qsize_and_empty(self, unbounded: MyQueue[int]):
        assert unbounded.empty()
        assert unbounded.qsize() == 0
        unbounded.put(1)
        unbounded.put(2)
        assert not unbounded.empty()
        assert unbounded.qsize() == 2
        unbounded.get()
        assert unbounded.qsize() == 1

    def test_maxsize_limit(self, bounded: MyQueue[int]):
        """maxsize=2 时最多存 2 个"""
        bounded.put(1)
        bounded.put(2)
        assert bounded.qsize() == 2

    def test_unbounded_queue_never_full(self, unbounded: MyQueue[int]):
        """maxsize=0 表示无限容量"""
        for i in range(1000):
            unbounded.put(i)
        assert unbounded.qsize() == 1000

    def test_generic_type(self, unbounded: MyQueue[str]):
        """泛型：存字符串取字符串"""
        unbounded.put("hello")
        assert unbounded.get() == "hello"


class TestNonBlocking:
    """非阻塞路径的异常"""

    def test_get_timeout_raises_empty(self, unbounded: MyQueue[int]):
        """空队列 get 超时抛 Empty，且确实等了约 0.1s"""
        start = time.monotonic()
        with pytest.raises(Empty):
            unbounded.get(timeout=0.1)
        assert time.monotonic() - start == pytest.approx(0.1, abs=0.05)

    def test_put_timeout_raises_full(self):
        """满队列 put 超时抛 Full"""
        bounded = MyQueue(maxsize=1)
        bounded.put(1)
        start = time.monotonic()
        with pytest.raises(Full):
            bounded.put(2, timeout=0.1)
        assert time.monotonic() - start == pytest.approx(0.1, abs=0.05)

    def test_timeout_does_not_corrupt_queue(self):
        """超时失败后队列状态不受影响"""
        bounded = MyQueue(maxsize=1)
        bounded.put(1)
        with pytest.raises(Full):
            bounded.put(2, timeout=0.05)
        assert bounded.qsize() == 1
        assert bounded.get() == 1

    def test_negative_timeout_raises_value_error(self, unbounded: MyQueue[int]):
        with pytest.raises(ValueError):
            unbounded.get(timeout=-1)
        # 与标准库一致：put 的 timeout 校验只在有界队列中触发
        bounded = MyQueue(maxsize=1)
        with pytest.raises(ValueError):
            bounded.put(1, timeout=-1)
        # 无界队列：负 timeout 被忽略，正常放入
        unbounded.put(1, timeout=-1)
        assert unbounded.qsize() == 1


class TestBlocking:
    """阻塞与唤醒"""

    def test_get_blocks_until_put(self, unbounded: MyQueue[int]):
        """消费者阻塞等待，生产者放入后被唤醒并拿到该元素"""
        result: list[int] = []

        def consumer():
            result.append(unbounded.get())  # 无限等待

        t = threading.Thread(target=consumer)
        t.start()
        time.sleep(0.1)  # 让消费者先进入 wait
        assert result == []  # 确认确实在阻塞
        unbounded.put(42)
        t.join(timeout=2)
        assert not t.is_alive()  # 没有卡死
        assert result == [42]

    def test_put_blocks_until_get(self):
        """生产者阻塞等待，消费者取走后被唤醒并成功放入"""
        q: MyQueue[int] = MyQueue(maxsize=1)
        q.put(1)
        done = threading.Event()

        def producer():
            q.put(2)  # 队列满，阻塞等待空位
            done.set()

        t = threading.Thread(target=producer)
        t.start()
        time.sleep(0.1)
        assert not done.is_set()  # 确认确实在阻塞
        assert q.get() == 1       # 腾出空间，唤醒生产者
        t.join(timeout=2)
        assert not t.is_alive()
        assert done.is_set()
        assert q.get() == 2

    def test_notify_wakes_exactly_one_consumer(self, unbounded: MyQueue[int]):
        """1 个元素只唤醒 1 个睡眠的消费者"""
        got: list[int] = []
        barrier = threading.Barrier(3, timeout=2)  # 确保 3 个消费者都已睡下

        def consumer():
            barrier.wait()
            try:
                got.append(unbounded.get(timeout=2))
            except Empty:
                pass

        threads = [threading.Thread(target=consumer) for _ in range(3)]
        for t in threads:
            t.start()
        time.sleep(0.05)
        unbounded.put(99)
        for t in threads:
            t.join(timeout=3)
        assert got == [99]  # 只有 1 个消费者拿到


class TestConcurrency:
    """多生产者多消费者压力测试"""

    def test_multi_producer_consumer_no_loss_no_dup(self):
        """3 生产者 × 2 消费者，10 项/人：不丢、不重、数量正确"""
        n_producers, n_consumers, n_items = 3, 2, 10
        q: MyQueue[int] = MyQueue(maxsize=3)
        consumed: list[int] = []
        lock = threading.Lock()

        def producer(pid: int):
            for i in range(n_items):
                q.put(pid * n_items + i)  # 队列满时自动阻塞

        def consumer():
            for _ in range(n_producers * n_items // n_consumers):
                item = q.get(timeout=5)
                with lock:
                    consumed.append(item)

        threads = [threading.Thread(target=producer, args=(p,)) for p in range(n_producers)]
        threads += [threading.Thread(target=consumer) for _ in range(n_consumers)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)
            assert not t.is_alive(), "存在未结束的线程，可能死锁"

        expected = [p * n_items + i for p in range(n_producers) for i in range(n_items)]
        assert sorted(consumed) == expected  # 不丢、不重

    def test_fifo_within_single_producer(self, unbounded: MyQueue[int]):
        """单个生产者的元素顺序保持 FIFO"""
        for i in range(50):
            unbounded.put(i)
        got: list[int] = []
        while not unbounded.empty():
            got.append(unbounded.get())
        assert got == list(range(50))
