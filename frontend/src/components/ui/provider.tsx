"use client"

import { MantineProvider } from '@mantine/core';
import React, { type PropsWithChildren } from "react"
import { system } from "../../theme"
import { ColorModeProvider } from "./color-mode"

export function CustomProvider(props: PropsWithChildren) {
  return (
    <MantineProvider value={system}>
      <ColorModeProvider defaultTheme="light">
        {props.children}
      </ColorModeProvider>
    </MantineProvider>
  )
}
