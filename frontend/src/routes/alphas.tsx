import {
    createFileRoute,
} from "@tanstack/react-router"

import { AlphaTable } from "@/components/alpha/table"
import { Container } from '@mantine/core'

export const Route = createFileRoute("/alphas")({
    component: Alphas,
})

function Alphas() {
    return (
        <>
            <Container size="lg" my="md">
                <AlphaTable />
            </Container>
        </>
    )
}
