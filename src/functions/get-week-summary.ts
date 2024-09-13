import dayjs from 'dayjs'
import { db } from '../db'
import { goalCompletions, goals } from '../db/schema'
import { and, count, lte, sql, gte, eq, desc } from 'drizzle-orm'

export async function getWeekSummary() {
  const firstDayOfWeek = dayjs().startOf('week').toDate()
  const lastDayOfWeek = dayjs().endOf('week').toDate()

  const goalsCreatedUpToWeek = db.$with('goals_created_up_to_week').as(
    db
      .select({
        id: goals.id,
        title: goals.title,
        desiredWeeklyFrequency: goals.desiredWeeklyFrequency,
        createdAt: goals.createdAt,
      })
      .from(goals)
      .where(lte(goals.createdAt, lastDayOfWeek))
  )
  const goalsCompletedInWeek = db.$with('goals.completed_in_week').as(
    db
      .select({
        id: goalCompletions.id,
        title: goals.title,
        completedAt: goalCompletions.createdAt,
        completionDate: sql`
            DATE(${goalCompletions.createdAt})
        `.as('completionDate'),
      })
      .from(goalCompletions)
      .innerJoin(goals, eq(goals.id, goalCompletions.goalId))

      .where(
        and(
          gte(goalCompletions.createdAt, firstDayOfWeek),
          lte(goalCompletions.createdAt, lastDayOfWeek)
        )
      )
      .orderBy(goalCompletions.createdAt)
  )

  const goalsCompleteByWeekDay = db.$with('goals.completed_by_week_day').as(
    db
      .select({
        completionDate: goalsCompletedInWeek.completionDate,
        completions: sql`
         JSON_AGG(
          JSON_BUILD_OBJECT(
            'id', ${goalsCompletedInWeek.id},
            'title', ${goalsCompletedInWeek.title},
            'completedAt', ${goalsCompletedInWeek.completedAt}
          )
        )
        `.as('completions'),
      })
      .from(goalsCompletedInWeek)
      .groupBy(goalsCompletedInWeek.completionDate)
      .orderBy(goalsCompletedInWeek.completionDate)
  )

  type GoalsPerDay = Record<
    string,
    {
      id: string
      title: string
      completedAt: string
    }[]
  >

  const result = await db
    .with(goalsCreatedUpToWeek, goalsCompletedInWeek, goalsCompleteByWeekDay)
    .select({
      completed: sql`
        (SELECT COUNT(*) FROM ${goalsCompletedInWeek})
        `.mapWith(Number),
      total: sql`
        (SELECT SUM(${goalsCreatedUpToWeek.desiredWeeklyFrequency}) FROM ${goalsCreatedUpToWeek})
  `.mapWith(Number),
      goalsPerDay: sql<GoalsPerDay>`
     JSON_OBJECT_AGG(
     ${goalsCompleteByWeekDay.completionDate},
     ${goalsCompleteByWeekDay.completions}
     )
`,
    })
    .from(goalsCompleteByWeekDay)

  return {
    summary: result[0],
  }
}
