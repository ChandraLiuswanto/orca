import { describe, expect, it } from 'vitest'
import { planWorktreeSortOrderUpdates } from './sort-order-update'

describe('planWorktreeSortOrderUpdates', () => {
  it('re-ranks only a row moved between kept neighbours', () => {
    const ranks: Record<string, number> = { a: 5_000, b: 4_000, c: 3_000, d: 2_000 }

    expect(
      planWorktreeSortOrderUpdates(['b', 'c', 'a', 'd'], (id) => ({ sortOrder: ranks[id] }), 9_000)
    ).toEqual([{ worktreeId: 'a', sortOrder: 2_500 }])
  })

  it('always leaves the saved order strictly descending in whole ranks', () => {
    let seed = 7
    const random = () => {
      seed = (seed * 1_103_515_245 + 12_345) % 2_147_483_648
      return seed / 2_147_483_648
    }
    const ids = Array.from({ length: 60 }, (_, index) => `w${index}`)
    const ranks: Record<string, number | undefined> = Object.fromEntries(
      ids.map((id, index) => [id, index % 17 === 0 ? undefined : 100_000 - index * 1_000])
    )
    const order = [...ids]
    for (let round = 0; round < 400; round += 1) {
      const moves = round % 50 === 0 ? 60 : 1 + Math.floor(random() * 3)
      for (let move = 0; move < moves; move += 1) {
        const [moved] = order.splice(Math.floor(random() * order.length), 1)
        order.splice(Math.floor(random() * (order.length + 1)), 0, moved)
      }
      for (const { worktreeId, sortOrder } of planWorktreeSortOrderUpdates(
        order,
        (id) => ({ sortOrder: ranks[id] }),
        1_000_000 + round
      )) {
        ranks[worktreeId] = sortOrder
      }
      expect(
        order.every(
          (id, index) =>
            Number.isInteger(ranks[id]) && (index === 0 || ranks[order[index - 1]]! > ranks[id]!)
        )
      ).toBe(true)
      expect(
        planWorktreeSortOrderUpdates(order, (id) => ({ sortOrder: ranks[id] }), 2_000_000)
      ).toEqual([])
    }
  })
})
