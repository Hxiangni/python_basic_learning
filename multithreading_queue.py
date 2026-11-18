import threading
import time
from queue import Empty, Full
from typing import Generic, TypeVar

# T类型占位符
T = TypeVar("T")


class MyQueue(Generic[T]):
    """线程安全的阻塞式 FIFO 队列（带条件变量，行为对齐 queue.Queue）"""

    def __init__(self, maxsize: int = 0) -> None:
        self.maxsize_: int = maxsize
        self.data_: list[T] = []
        # 全队列唯一的一把互斥锁，保护 data_ 的所有访问
        self.mutex_: threading.Lock = threading.Lock()
        # 两个条件变量共用这把锁：Condition 的构造参数就是它内部使用的锁
        # 于是 with self.not_full_ / with self.not_empty_ 拿到的都是 mutex_
        # （对齐标准库 queue.Queue：一把锁 + 多个条件变量）
        self.not_full_: threading.Condition = threading.Condition(self.mutex_)
        # not_empty_: 消费者在"队列不空"时才能 get
        self.not_empty_: threading.Condition = threading.Condition(self.mutex_)

    '''
    一个对象能被 `with` 使用，必须实现两个方法：
    - __enter__()：进入with块的时候执行
    - __exit__：离开 with 块时执行（正常结束 / 抛异常都会进），负责清理

    with OBJ as VAR:
    1.创建OBJ对象
    2.调用OBJ.__enter__()，函数返回值给as后的VAR若没有返回值则不写或者丢弃
    3.执行with块中的代码
    4.调用OBJ.__exit__()

    Condition.wait() 的行为：
    1. 先释放自己持有的锁（让别的线程有机会修改条件）
    2. 阻塞休眠，直到被 notify() 唤醒或超时
    3. 醒来后重新抢锁，抢到才从 wait() 返回
    '''
    def put(self, item: T, timeout: float | None = None) -> None:
        with self.not_full_:
            # 必须用 while 而不是 if：被唤醒后条件可能又被其他线程改变，
            # 醒来后要重新检查条件，不满足就继续睡，防止虚假唤醒
            if self.maxsize_ > 0:
                # 如果传入的是None表示没有指定实现,无限期等待条件满足
                if timeout is None:
                    while len(self.data_) >= self.maxsize_:
                        # 队列已满，释放锁等待被唤醒
                        # 被唤醒后继续while判断条件是否满足
                        # 条件不满足为flase跳出循环
                        '''
                        wait()
                        前置检查,没持有锁就调用wait()编程错误直接报错
                        给当前线程造一个新锁然后锁住
                        私人锁登记到等待队列中
                        最后释放抢到的锁等待被唤醒
                        醒来后继续抢锁抢不到继续阻塞
                        '''
                        _ = self.not_full_.wait()  
                elif timeout < 0:
                    raise ValueError("'timeout' 必须是非负数")
                else: #timeout>0的情况
                    # end时间=当前时间 + 传入的超时时间
                    endtime = time.monotonic() + timeout
                    # while 循环：wait 醒来后必须重新检查条件（防虚假唤醒）
                    # wait() 返回值不用于判断超时，超时由 remaining <= 0 判定
                    while len(self.data_) >= self.maxsize_:
                        remaining = endtime - time.monotonic()
                        if remaining <= 0.0:
                            # raise 主动抛出异常 类比C++的thorw
                            raise Full(f"等待 {timeout}s 后队列仍还是满的")

                        # 还有剩余时间就继续给我等就行了
                        _ = self.not_full_.wait(remaining)

            # 历经重重检查获取锁，满足有限的时间，满足队列不满终于可以放了
            self.data_.append(item)
            # 唤醒一个正在等待"队列不空"的消费者
            self.not_empty_.notify()

    def get(self, timeout: float | None = None) -> T:
        with self.not_empty_:

            if timeout is None:
                while not self.data_:
                    _ = self.not_empty_.wait()  # 队列为空，无限期等待
            elif timeout < 0:
                raise ValueError("'timeout' 必须是非负数")
            else: # timeout > 0
                endtime = time.monotonic() + timeout
                while not self.data_:
                    remaining = endtime - time.monotonic()
                    if remaining <= 0.0:
                        raise Empty(f"等待 {timeout}s 后队列仍为空")
                    _ = self.not_empty_.wait(remaining)
            item = self.data_.pop(0)
            # 唤醒一个正在等待"队列不满"的生产者
            self.not_full_.notify()
            return item

    def empty(self) -> bool:
        with self.mutex_:
            return len(self.data_) == 0

    def qsize(self) -> int:
        with self.mutex_:
            return len(self.data_)
