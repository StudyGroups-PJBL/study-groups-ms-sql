import { randomUUID } from 'crypto';
import amqp, { Channel, ChannelModel } from 'amqplib';
import { IEventPublisher } from '../../domain/events/IEventPublisher';
import { environment } from '../config/environment';

export class RabbitMqPublisher implements IEventPublisher {
  private readonly rabbitMqUrl: string;
  private readonly exchangeName: string = 'study_groups.events';
  private readonly exchangeType: string = 'topic';
  private connection: ChannelModel | null = null;
  private channel: Channel | null = null;

  constructor(rabbitMqUrl: string = environment.rabbitMqUrl) {
    this.rabbitMqUrl = rabbitMqUrl;
  }

  private async getChannel(): Promise<Channel | null> {
    if (this.channel) {
      return this.channel;
    }

    if (!this.rabbitMqUrl || this.rabbitMqUrl.trim().length === 0) {
      console.warn(
        'URL do RabbitMQ não configurada. Operando em modo de fallback local para publicação de eventos.'
      );
      return null;
    }

    const conn = await amqp.connect(this.rabbitMqUrl);
    this.connection = conn;

    conn.on('error', (err: Error) => {
      console.error(`Erro na conexão com o broker RabbitMQ: ${err.message}`);
      this.channel = null;
      this.connection = null;
    });

    conn.on('close', () => {
      console.warn('Conexão com o broker RabbitMQ encerrada.');
      this.channel = null;
      this.connection = null;
    });

    const ch = await conn.createChannel();
    await ch.assertExchange(this.exchangeName, this.exchangeType, { durable: true });
    this.channel = ch;

    return this.channel;
  }

  public async publish<T>(eventName: string, payload: T): Promise<void> {
    const routingKey = eventName || 'task.status.changed';
    const nowIso = new Date().toISOString();
    const messageBuffer = Buffer.from(
      JSON.stringify({
        eventId: randomUUID(),
        event: routingKey,
        eventType: routingKey,
        exchange: this.exchangeName,
        timestamp: nowIso,
        occurredAt: nowIso,
        data: payload,
        payload
      })
    );

    try {
      const channel = await this.getChannel();

      if (!channel) {
        console.log(
          `Fallback de mensageria acionado para o evento [${routingKey}] no exchange [${this.exchangeName}]:`,
          JSON.stringify(payload)
        );
        return;
      }

      channel.publish(this.exchangeName, routingKey, messageBuffer, {
        persistent: true,
        contentType: 'application/json'
      });

      console.log(
        `Evento [${routingKey}] publicado com sucesso no exchange [${this.exchangeName}] do RabbitMQ.`
      );
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Falha desconhecida no broker';
      console.warn(
        `Broker RabbitMQ indisponível (${errorMessage}). Evento [${routingKey}] registrado em fallback seguro:`,
        JSON.stringify(payload)
      );
      this.channel = null;
      this.connection = null;
    }
  }

  public async close(): Promise<void> {
    try {
      if (this.channel) {
        await this.channel.close();
        this.channel = null;
      }
      if (this.connection) {
        await this.connection.close();
        this.connection = null;
      }
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Erro ao fechar conexão';
      console.warn(`Aviso ao encerrar conexão com o RabbitMQ: ${errorMessage}`);
    }
  }
}
