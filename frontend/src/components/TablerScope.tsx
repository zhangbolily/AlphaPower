// TablerScope.tsx
import { PropsWithChildren } from 'react'

type TablerScopeProps = PropsWithChildren<{
  theme?: 'light' | 'dark'
}>

/** 在 .tb 容器内写 Tabler 的 HTML 片段与 class，不影响全局/Chakra */
export function TablerScope({ children, theme = 'light' }: TablerScopeProps) {
  return (
    <section className="tb" data-bs-theme={theme}>
      {children}
    </section>
  )
}
