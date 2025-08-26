import { useState } from 'react';
import cx from 'clsx';
import { Checkbox, ScrollArea, Table, Badge } from '@mantine/core';
import classes from './table.module.css';

const data = [
    {
        id: 'xjfjfsj',
        name: 'xjfjfsj',
        type: 'Regular',
        competition: 'PPAC2025',
        language: 'FAST EXPRESSION',
        date_created: '2024-10-01',
        date_submitted: '2024-10-02',
        favorite: false
    },
];

export function AlphaTable() {
    const [selection, setSelection] = useState(['xjfjfsj']);
    const toggleRow = (id: string) =>
        setSelection((current) =>
            current.includes(id) ? current.filter((item) => item !== id) : [...current, id]
        );
    const toggleAll = () =>
        setSelection((current) => (current.length === data.length ? [] : data.map((item) => item.id)));

    const rows = data.map((item) => {
        const selected = selection.includes(item.id);
        return (
            <Table.Tr key={item.id} className={cx({ [classes.rowSelected]: selected })}>
                <Table.Td>
                    <Checkbox checked={selection.includes(item.id)} onChange={() => toggleRow(item.id)} />
                </Table.Td>
                <Table.Td>{item.id}</Table.Td>
                <Table.Td>{item.name}</Table.Td>
                <Table.Td><Badge>{item.type}</Badge></Table.Td>
                <Table.Td>
                    <Badge>{item.competition}</Badge>
                </Table.Td>
                <Table.Td>
                    <Badge>{item.language}</Badge>
                </Table.Td>
                <Table.Td>{item.date_created}</Table.Td>
                <Table.Td>{item.date_submitted}</Table.Td>
                <Table.Td>
                    <Checkbox
                        checked={item.favorite}
                        onChange={() => {
                            // Handle favorite toggle
                        }}
                    />
                </Table.Td>
            </Table.Tr>
        );
    });

    return (
        <ScrollArea>
            <Table miw={800} verticalSpacing="sm">
                <Table.Thead>
                    <Table.Tr>
                        <Table.Th w={40}>
                            <Checkbox
                                onChange={toggleAll}
                                checked={selection.length === data.length}
                                indeterminate={selection.length > 0 && selection.length !== data.length}
                            />
                        </Table.Th>
                        <Table.Th>ID</Table.Th>
                        <Table.Th>Name</Table.Th>
                        <Table.Th>Type</Table.Th>
                        <Table.Th>Competition</Table.Th>
                        <Table.Th>Languange</Table.Th>
                        <Table.Th>Date Created</Table.Th>
                        <Table.Th>Date Submitted</Table.Th>
                        <Table.Th>Favorite</Table.Th>
                    </Table.Tr>
                </Table.Thead>
                <Table.Tbody>{rows}</Table.Tbody>
            </Table>
        </ScrollArea>
    );
}