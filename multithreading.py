import multiprocessing
import time
from typing_extensions import override

class MyProcess(multiprocessing.Process):
    def __init__(self, name: str, delay: float) -> None:
        super().__init__()
        self.name: str = name
        self.delay: float = delay
    
    # 重写父类方法时显式加上override(C++的override)
    @override
    def run(self):
        # 进程启动后自动执行run方法
        print(f"自定义进程 {self.name} 启动 PID:{self.pid}")
        time.sleep(self.delay)
        print(f"自定义进程 {self.name} 结束")

if __name__ == '__main__':
    p = MyProcess("自定义进程C", 1.5)
    p.start()
    p.join()
    print("主进程结束")