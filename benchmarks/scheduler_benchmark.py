import asyncio
import time
from typing import List

from alphapower.engine.simulation.task.scheduler import PriorityScheduler
from alphapower.internal.entity import SimulationTask


async def benchmark_scheduler():
    """
    基准测试 PriorityScheduler 在大任务量场景下的性能。
    """
    # 创建大量模拟任务
    num_tasks = 100000
    tasks = [
        SimulationTask(
            id=i,
            priority=i % 10,  # 模拟不同优先级
            settings_group_key=f"group_{i % 5}",  # 模拟分组
        )
        for i in range(num_tasks)
    ]

    # 初始化调度器
    scheduler = PriorityScheduler(tasks=tasks)

    # 开始基准测试
    batch_size = 10
    start_time = time.time()
    schedule_result: List[List[SimulationTask]] = []

    while await scheduler.has_tasks():
        scheduled_tasks = await scheduler.schedule(batch_size=batch_size)
        schedule_result.append(scheduled_tasks)

    end_time = time.time()
    print(f"调度 {num_tasks} 个任务完成，耗时 {end_time - start_time:.2f} 秒")

    # 验证调度结果
    validate_schedule_result(schedule_result)


def validate_schedule_result(schedule_result: List[List[SimulationTask]]):
    """
    验证调度结果是否正确。
    """
    for scheduled_tasks in schedule_result:
        for i in range(1, len(scheduled_tasks)):
            assert scheduled_tasks[i].priority <= scheduled_tasks[i - 1].priority
            for j in range(1, len(scheduled_tasks)):
                assert scheduled_tasks[j].priority <= scheduled_tasks[j - 1].priority

            for task in scheduled_tasks:
                assert task.settings_group_key == scheduled_tasks[0].settings_group_key


if __name__ == "__main__":
    asyncio.run(benchmark_scheduler())
