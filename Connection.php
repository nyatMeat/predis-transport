<?php

declare(strict_types=1);

namespace Symfony\Component\Messenger\Bridge\Redis\PredisTransport;

use Predis\Client;
use Predis\ClientException;
use Predis\PredisException;
use Symfony\Component\Messenger\Exception\InvalidArgumentException;
use Symfony\Component\Messenger\Exception\LogicException;
use Symfony\Component\Messenger\Exception\TransportException;

class Connection
{
    private const DEFAULT_OPTIONS = [
        'host' => '127.0.0.1',
        'port' => 6379,
        'stream' => 'messages',
        'group' => 'symfony',
        'consumer' => 'consumer',
        'auto_setup' => true,
        'delete_after_ack' => true,
        'delete_after_reject' => true,
        'stream_max_entries' => 0, // any value higher than 0 defines an approximate maximum number of stream entries
        'dbindex' => 0,
        'redeliver_timeout' => 3600, // Timeout before redeliver messages still in pending state (seconds)
        'claim_interval' => 60000, // Interval by which pending/abandoned messages should be checked
        'auth' => null,
        'serializer' => 1, // see \Redis::SERIALIZER_PHP,
        'sentinel_master' => null, // String, master to look for (optional, default is NULL meaning Sentinel support is disabled)
        'timeout' => 0.0, // Float, value in seconds (optional, default is 0 meaning unlimited)
        'read_timeout' => 0.0, //  Float, value in seconds (optional, default is 0 meaning unlimited)
        'retry_interval' => 0, //  Int, value in milliseconds (optional, default is 0)
        'persistent_id' => null, // String, persistent connection id (optional, default is NULL meaning not persistent)
        'ssl' => null, // see https://php.net/context.ssl
        'sentinel_timeout' => 0, // Sentinel connection timeout,
        'update_sentinels' => false, // Is need update sentinels
        'sentinel_retry_limit' => 20,
        'sentinel_retry_wait' => 1000,
    ];

    private Client $redis;

    private string $stream;

    private string $queue;

    private string $group;

    private string $consumer;

    private bool $autoSetup;

    private int $maxEntries;

    private int $redeliverTimeout;

    private float $nextClaim = 0.0;

    private float $claimInterval;

    private bool $deleteAfterAck;

    private bool $deleteAfterReject;

    private bool $couldHavePendingMessages = true;

    public function __construct(array $options, ?Client $redis = null)
    {
        $options += self::DEFAULT_OPTIONS;

        foreach (['stream', 'group', 'consumer'] as $key) {
            if ('' === $options[$key]) {
                throw new InvalidArgumentException(sprintf('"%s" should be configured, got an empty string.', $key));
            }
        }

        $this->stream = $options['stream'];
        $this->group = $options['group'];
        $this->consumer = $options['consumer'];
        $this->queue = $this->stream . '__queue';
        $this->autoSetup = $options['auto_setup'];
        $this->maxEntries = $options['stream_max_entries'];
        $this->deleteAfterAck = $options['delete_after_ack'];
        $this->deleteAfterReject = $options['delete_after_reject'];
        $this->redeliverTimeout = $options['redeliver_timeout'] * 1000;
        $this->claimInterval = $options['claim_interval'] / 1000;
    }

    private function claimOldPendingMessages(): void
    {
        try {
            $pendingMessages = $this->redis->executeCommand($this->redis->createCommand('xpending', [$this->stream, $this->group, '-', '+', 1])) ?: [];
        } catch (PredisException $e) {
            throw new TransportException($e->getMessage(), 0, $e);
        }

        $claimableIds = [];

        foreach ($pendingMessages as $pendingMessage) {
            if ($pendingMessage[1] === $this->consumer) {
                $this->couldHavePendingMessages = true;

                return;
            }

            if ($pendingMessage[2] >= $this->redeliverTimeout) {
                $claimableIds[] = $pendingMessage[0];
            }
        }

        if (\count($claimableIds) > 0) {
            try {
                $this->redis->executeCommand(
                    $this->redis->createCommand(
                        'xclaim',
                        [
                            $this->stream,
                            $this->group,
                            $this->consumer,
                            $this->redeliverTimeout,
                            $claimableIds,
                            'JUSTID',
                        ],
                    )
                );

                $this->couldHavePendingMessages = true;
            } catch (ClientException $e) {
                throw new TransportException($e->getMessage(), 0, $e);
            }
        }

        $this->nextClaim = microtime(true) + $this->claimInterval;
    }

    public function get(): ?array
    {
        if ($this->autoSetup) {
            $this->setup();
        }

        $now = microtime();
        $now = substr($now, 11) . substr($now, 2, 3);

        $queuedMessageCount = $this->redis->zcount($this->queue, 0, $now) ?? 0;

        while ($queuedMessageCount--) {
            if (!$message = $this->redis->zpopmin($this->queue, 1)) {
                break;
            }

            [$queuedMessage, $expiry] = $message;

            if (\strlen($expiry) === \strlen($now) ? $expiry > $now : \strlen($expiry) < \strlen($now)) {
                // if a future-placed message is popped because of a race condition with
                // another running consumer, the message is readded to the queue

                if (!$this->redis->zadd($this->queue, 'NX', $expiry, $queuedMessage)) {
                    throw new TransportException('Could not add a message to the redis stream.');
                }

                break;
            }

            $decodedQueuedMessage = json_decode($queuedMessage, true);
            $this->add(\array_key_exists('body', $decodedQueuedMessage) ? $decodedQueuedMessage['body'] : $queuedMessage, $decodedQueuedMessage['headers'] ?? [], 0);
        }

        if (!$this->couldHavePendingMessages && $this->nextClaim <= microtime(true)) {
            $this->claimOldPendingMessages();
        }

        $messageId = '>'; // will receive new messages

        if ($this->couldHavePendingMessages) {
            $messageId = '0'; // will receive consumers pending messages
        }

        try {
            $messages = $this->redis->executeCommand(
                $this->redis->createCommand(
                    'xreadgroup',
                    [
                        $this->group,
                        $this->consumer,
                        [$this->stream => $messageId],
                        1,
                        1,
                    ],
                )
            );
        } catch (PredisException $e) {
            throw new TransportException($e->getMessage(), 0, $e);
        }

        if (false === $messages) {
            throw new TransportException('Could not read messages from the redis stream.');
        }

        if ($this->couldHavePendingMessages && empty($messages[$this->stream])) {
            $this->couldHavePendingMessages = false;

            // No pending messages so get a new one
            return $this->get();
        }

        foreach ($messages[$this->stream] ?? [] as $key => $message) {
            return [
                'id' => $key,
                'data' => $message,
            ];
        }

        return null;
    }

    public function ack(string $id): void
    {
        try {
            $acknowledged = $this->redis->executeCommand(
                $this->redis->createCommand(
                    'xack',
                    [
                        $this->stream,
                        $this->group,
                        $id,
                    ],
                )
            );

            if ($this->deleteAfterAck) {
                $acknowledged = $this->redis->xdel($this->stream, $id);
            }
        } catch (PredisException $e) {
            throw new TransportException($e->getMessage(), 0, $e);
        }

        if (!$acknowledged) {
            throw new TransportException(sprintf('Could not acknowledge redis message "%s".', $id));
        }
    }

    public function reject(string $id): void
    {
        try {
            $deleted = $this->redis->executeCommand(
                $this->redis->createCommand(
                    'xack',
                    [
                        $this->stream,
                        $this->group,
                        $id,
                    ],
                ),
            );

            if ($this->deleteAfterReject) {
                $deleted = $this->redis->xdel($this->stream, $id) && $deleted;
            }
        } catch (PredisException $e) {
            throw new TransportException($e->getMessage(), 0, $e);
        }

        if (!$deleted) {
            throw new TransportException(sprintf('Could not delete message "%s" from the redis stream.', $id));
        }
    }

    public function add(string $body, array $headers, int $delayInMs = 0): string
    {
        if ($this->autoSetup) {
            $this->setup();
        }

        try {
            if ($delayInMs > 0) { // the delay is <= 0 for queued messages
                $id = uniqid('', true);

                $message = json_encode([
                    'body' => $body,
                    'headers' => $headers,
                    // Entry need to be unique in the sorted set else it would only be added once to the delayed messages queue
                    'uniqid' => $id,
                ]);

                if (false === $message) {
                    throw new TransportException(json_last_error_msg());
                }

                $now = explode(' ', microtime(), 2);
                $now[0] = str_pad($delayInMs + substr($now[0], 2, 3), 3, '0', \STR_PAD_LEFT);
                if (3 < \strlen($now[0])) {
                    $now[1] += substr($now[0], 0, -3);
                    $now[0] = substr($now[0], -3);

                    if (\is_float($now[1])) {
                        throw new TransportException("Message delay is too big: {$delayInMs}ms.");
                    }
                }

                $added = $this->rawCommand('ZADD', 'NX', $now[1] . $now[0], $message);
            } else {
                $message = json_encode([
                    'body' => $body,
                    'headers' => $headers,
                ]);

                if (false === $message) {
                    throw new TransportException(json_last_error_msg());
                }

                if ($this->maxEntries) {
                    $added = $this->redis->xadd($this->stream, '*', ['message' => $message], $this->maxEntries, true);
                } else {
                    $added = $this->redis->xadd($this->stream, '*', ['message' => $message]);
                }

                $id = $added;
            }
        } catch (PredisException $e) {
            throw new TransportException($e->getMessage(), 0, $e);
        }

        if (!$added) {
            throw new TransportException('Could not add a message to the redis stream.');
        }

        return $id;
    }

    public function setup(): void
    {
        try {
            $this->redis->executeCommand($this->redis->createCommand('xgroup', ['CREATE', $this->stream, $this->group, 0, 1]));
        } catch (PredisException $e) {
            throw new TransportException($e->getMessage(), 0, $e);
        }

        if ($this->deleteAfterAck || $this->deleteAfterReject) {
            $groups = $this->redis->executeCommand(
                $this->redis->createCommand(
                    'xinfo',
                    [
                        'GROUPS',
                        $this->stream,
                    ],
                )
            );
            if (
                // support for Redis extension version 5+
                (\is_array($groups) && 1 < \count($groups))
                // support for Redis extension version 4.x
                || (\is_string($groups) && substr_count($groups, '"name"'))
            ) {
                throw new LogicException(sprintf('More than one group exists for stream "%s", delete_after_ack and delete_after_reject cannot be enabled as it risks deleting messages before all groups could consume them.', $this->stream));
            }
        }

        $this->autoSetup = false;
    }

    public function cleanup(): void
    {
        static $unlink = true;

        if ($unlink) {
            try {
                $unlink = false !== $this->redis->executeCommand(
                        $this->redis->createCommand(
                            'unlink',
                            [
                                $this->stream,
                                $this->queue,
                            ],
                        )
                    );
            } catch (\Throwable) {
                $unlink = false;
            }
        }

        if (!$unlink) {
            // in case of cluster
            foreach ([$this->stream, $this->queue] as $key) {
                $this->redis->del($key);
            }
        }
    }

    public function getMessageCount(): int
    {
        $groups = $this->redis->xinfo('GROUPS', $this->stream) ?: [];

        $lastDeliveredId = null;

        foreach ($groups as $group) {
            if ($group['name'] !== $this->group) {
                continue;
            }

            // Use "lag" key provided by Redis 7.x. See https://redis.io/commands/xinfo-groups/#consumer-group-lag.
            if (isset($group['lag'])) {
                return $group['lag'];
            }

            if (!isset($group['last-delivered-id'])) {
                return 0;
            }

            $lastDeliveredId = $group['last-delivered-id'];
            break;
        }

        if (null === $lastDeliveredId) {
            return 0;
        }

        $total = 0;

        while (true) {
            if (!$range = $this->redis->xRange($this->stream, $lastDeliveredId, '+', 100)) {
                return $total;
            }

            $total += \count($range);

            $lastDeliveredId = preg_replace_callback('#\d+$#', static fn(array $matches) => (int) $matches[0] + 1, array_key_last($range));
        }
    }

    private function rawCommand(string $command, ...$arguments): mixed
    {
        try {
            $result = $this->redis->rawCommand($this->queue, $command, $this->queue, ...$arguments);
        } catch (PredisException $e) {
            throw new TransportException($e->getMessage(), 0, $e);
        }

        if (false === $result) {
            throw new TransportException(sprintf('Could not run "%s" on Redis queue.', $command));
        }

        return $result;
    }
}