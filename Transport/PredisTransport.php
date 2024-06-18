<?php

declare(strict_types=1);

namespace Symfony\Component\Messenger\Bridge\Predis\Transport;

use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Transport\Receiver\MessageCountAwareInterface;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\SetupableTransportInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

class PredisTransport implements TransportInterface, SetupableTransportInterface, MessageCountAwareInterface
{
    private SerializerInterface $serializer;
    private PredisReceiver $receiver;
    private PredisSender $sender;

    public function __construct(
        private Connection $connection,
        ?SerializerInterface $serializer = null,
    ) {
        $this->serializer = $serializer ?? new PhpSerializer();
    }

    public function get(): iterable
    {
        return $this->getReceiver()->get();
    }

    public function ack(Envelope $envelope): void
    {
        $this->getReceiver()->ack($envelope);
    }

    public function reject(Envelope $envelope): void
    {
        $this->getReceiver()->reject($envelope);
    }

    public function send(Envelope $envelope): Envelope
    {
        return $this->getSender()->send($envelope);
    }

    public function setup(): void
    {
        $this->connection->setup();
    }

    public function getMessageCount(): int
    {
        return $this->getReceiver()->getMessageCount();
    }

    private function getReceiver(): PredisReceiver
    {
        return $this->receiver ??= new PredisReceiver($this->connection, $this->serializer);
    }

    private function getSender(): PredisSender
    {
        return $this->sender ??= new PredisSender($this->connection, $this->serializer);
    }
}