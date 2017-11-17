require('dotenv').config({ path: '.env', silent: true })

const amqp = require('amqplib')
const { logger } = require('./logger')
const { AMQP_ENDPOINT, QUEUE } = process.env

consume()

async function consume() {
  try {
    const connection = await amqp.connect(AMQP_ENDPOINT)

    connection.on('error', onError)
    connection.on('close', onClose)

    const channel = await connection.createChannel()
    await channel.assertQueue(QUEUE)

    channel.consume(QUEUE, message => {
      if (message !== null) {
        logger.info(Buffer.from(message))

        channel.ack(message)
      }
    })
  } catch (e) {
    logger.warn(e)
  }
}

function onClose(e) {
  logger.info('close')
  logger.error(e)

  setTimeout(consume, 5000)
}

function onError(e) {
  logger.info('error')
  logger.error(e)

  setTimeout(consume, 5000)
}
