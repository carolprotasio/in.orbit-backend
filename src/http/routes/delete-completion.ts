import { eq } from 'drizzle-orm'
import type { FastifyPluginAsyncZod } from 'fastify-type-provider-zod'
import z from 'zod'
import { db } from '../../db'
import { goalCompletions } from '../../db/schema'

export const deleteCompletionRoute: FastifyPluginAsyncZod = async app => {
  app.delete(
    '/deleteCompletions/:id',
    {
      schema: {
        params: z.object({
          id: z.string(),
        }),
      },
    },
    async (request, reply) => {
      const { id } = request.params

      await db.delete(goalCompletions).where(eq(goalCompletions.id, id))
    }
  )
}
