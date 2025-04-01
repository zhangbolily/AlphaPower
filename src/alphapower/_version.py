"""AlphaPower 的版本信息。

此模块处理 AlphaPower 的版本编号，
遵循标准版本方案，包含主版本号、次版本号、微版本号
和发布级别组件。
"""

from typing import Final, NamedTuple

__all__ = ("__version__", "__version_info__")


class Version(NamedTuple):
    """定义标准化的版本结构。

    复制 sys.version_info 的行为，使用结构化版本
    号，包含主版本号、次版本号、微版本号组件、
    发布级别和序列号。用于跟踪 AlphaPower 包的版本。

    属性:
        major: 表示主版本号的整数。
        minor: 表示次版本号的整数。
        micro: 表示微版本号的整数。
        releaselevel: 表示发布状态的字符串（'alpha'、'beta'、'candidate'、'final'）。
        serial: 用于发布序列标识的整数。稳定版本通常为 0。
    """

    major: int
    minor: int
    micro: int
    releaselevel: str  # Literal['alpha', 'beta', 'candidate', 'final']
    serial: int

    def _rl_shorthand(self) -> str:
        """返回发布级别的缩写表示。

        返回:
            一个包含当前发布级别缩写表示的字符串：
            'a' 代表 alpha，'b' 代表 beta，'rc' 代表 candidate。

        抛出:
            KeyError: 如果 releaselevel 不是 'alpha'、'beta' 或 'candidate' 之一。
        """
        return {
            "alpha": "a",
            "beta": "b",
            "candidate": "rc",
        }[self.releaselevel]

    def __str__(self) -> str:
        """返回版本的字符串表示。

        格式遵循标准版本方案：
        - 如果 micro 为 0 且 releaselevel 为 'final'，则为 major.minor
        - 如果 micro 不为 0 且 releaselevel 为 'final'，则为 major.minor.micro
        - 否则为 major.minor[.micro]发布级别缩写[序列号]

        返回:
            格式化的版本字符串。
        """
        version = f"{self.major}.{self.minor}"
        if self.micro != 0:
            version = f"{version}.{self.micro}"
        if self.releaselevel != "final":
            version = f"{version}{self._rl_shorthand()}{self.serial}"

        return version


__version_info__: Final[Version] = Version(
    major=0, minor=1, micro=0, releaselevel="alpha", serial=0
)
__version__: Final[str] = str(__version_info__)
