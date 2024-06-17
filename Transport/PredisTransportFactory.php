<?php

declare(strict_types=1);

namespace Symfony\Component\Messenger\Bridge\Redis\PredisTransport;

use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

class PredisTransportFactory
{
    public function createTransport(#[\SensitiveParameter] string $dsn, array $options, SerializerInterface $serializer): TransportInterface
    {
        unset($options['transport_name']);

        return new PredisTransport(Connection::fromDsn($dsn, $options), $serializer);
    }

    public function supports(#[\SensitiveParameter] string $dsn, array $options): bool
    {
        return str_starts_with($dsn, 'predis:');
    }
}