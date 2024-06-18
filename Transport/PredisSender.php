<?php

declare(strict_types=1);

namespace Symfony\Component\Messenger\Bridge\Predis\Transport;

use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Sender\SenderInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

class PredisSender implements SenderInterface
{
    public function __construct(
        private Connection $connection,
        private SerializerInterface $serializer,
    ) {
    }

    public function send(Envelope $envelope): Envelope
    {
        $encodedMessage = $this->serializer->encode($envelope);

        /** @var DelayStamp|null $delayStamp */
        $delayStamp = $envelope->last(DelayStamp::class);
        $delayInMs = null !== $delayStamp ? $delayStamp->getDelay() : 0;

        $id = $this->connection->add($encodedMessage['body'], $encodedMessage['headers'] ?? [], $delayInMs);

        return $envelope->with(new TransportMessageIdStamp($id));
    }
}