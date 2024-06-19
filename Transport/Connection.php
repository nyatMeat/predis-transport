<?php

declare(strict_types=1);

namespace Symfony\Component\Messenger\Bridge\Predis\Transport;

use Predis\Client;
use Predis\ClientException;
use Predis\PredisException;
use Symfony\Component\Messenger\Bridge\Predis\Transport\Predis\UNLINK;
use Symfony\Component\Messenger\Bridge\Predis\Transport\Predis\XACK;
use Symfony\Component\Messenger\Bridge\Predis\Transport\Predis\XCLAIM;
use Symfony\Component\Messenger\Bridge\Predis\Transport\Predis\XGROUP;
use Symfony\Component\Messenger\Bridge\Predis\Transport\Predis\XINFO;
use Symfony\Component\Messenger\Bridge\Predis\Transport\Predis\XPENDING;
use Symfony\Component\Messenger\Bridge\Predis\Transport\Predis\XREADGROUP;
use Symfony\Component\Messenger\Exception\InvalidArgumentException;
use Symfony\Component\Messenger\Exception\LogicException;
use Symfony\Component\Messenger\Exception\TransportException;

/**
 * @see
 */
class Connection
{
    private const DEFAULT_OPTIONS = [
        'dsn_list' => '127.0.0.1:6379',
        'stream' => 'messages',
        'group' => 'symfony',
        'consumer' => 'consumer',
        'auto_setup' => true,
        'delete_after_ack' => true,
        'delete_after_reject' => true,
        'stream_max_entries' => 0, // any value higher than 0 defines an approximate maximum number of stream entries
        'db_index' => 0,
        'redeliver_timeout' => 3600, // Timeout before redeliver messages still in pending state (seconds)
        'claim_interval' => 60000, // Interval by which pending/abandoned messages should be checked
        'username' => null,
        'password' => null,
        'sentinel_master' => null, // String, master to look for (optional, default is NULL meaning Sentinel support is disabled)
        'timeout' => 0.0, // Float, value in seconds (optional, default is 0 meaning unlimited)
        'read_timeout' => 0.0, //  Float, value in seconds (optional, default is 0 meaning unlimited)
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

    public function __construct(array $options)
    {
        $options += self::DEFAULT_OPTIONS;

        $this->redis = $this->initiateRedisFromOption($options);

        foreach (['stream', 'group', 'consumer'] as $key) {
            if ('' === $options[$key]) {
                throw new InvalidArgumentException(sprintf('"%s" should be configured, got an empty string.', $key));
            }
        }

        $this->stream = $options['stream'];
        $this->group = $options['group'];
        $this->consumer = $options['consumer'];
        $this->queue = $this->stream . '__queue';
        $this->autoSetup = (bool) $options['auto_setup'];
        $this->maxEntries = (int) $options['stream_max_entries'];
        $this->deleteAfterAck = (bool) $options['delete_after_ack'];
        $this->deleteAfterReject = (bool) $options['delete_after_reject'];
        $this->redeliverTimeout = $options['redeliver_timeout'] * 1000;
        $this->claimInterval = $options['claim_interval'] / 1000;
    }

    private function initiateRedisFromOption(array $options): Client
    {
        $clientOptions = [];

        $hosts = explode(',', $options['dsn_list']);

        if (!$hosts) {
            throw new InvalidArgumentException('Host list is empty');
        }

        $isSentinelConnection = $options['sentinel_master'];

        if (count($hosts) === 1) {
            $clientParameters = $hosts[0];
        } else {
            if ($isSentinelConnection) {
                $clientOptions['replication'] = 'sentinel';
                $clientOptions['service'] = $options['sentinel_master'];
            } else {
                $clientOptions['cluster'] = 'redis';
            }

            $clientParameters = $hosts;
        }

        if ($options['username']) {
            $clientOptions['parameters']['username'] = $options['username'];
        }

        if ($options['password']) {
            $clientOptions['parameters']['password'] = $options['password'];
        }

        $clientOptions['parameters']['timeout'] = (float) $options['timeout'];
        $clientOptions['parameters']['read_write_timeout'] = (float) $options['read_timeout'];
        $clientOptions['parameters']['database'] = (int) $options['db_index'];

        $clientOptions['throw_errors'] = true;

        $clientOptions['commands'] = [
            'XGROUP' => XGROUP::class,
            'UNLINK' => UNLINK::class,
            'XACK' => XACK::class,
            'XCLAIM' => XCLAIM::class,
            'XINFO' => XINFO::class,
            'XREADGROUP' => XREADGROUP::class,
            'XPENDING' => XPENDING::class,
        ];

        $client = new Client($clientParameters, $clientOptions);

        if ($isSentinelConnection) {
            $client->getConnection()->setRetryLimit((int) $options['sentinel_retry_limit']);
            $client->getConnection()->setUpdateSentinels((bool) $options['update_sentinels']);
            $client->getConnection()->setSentinelTimeout((float) $options['sentinel_timeout']);
        }

        return $client;
    }

    public static function fromDsn(string $dsn): self
    {
        $options = [];

        $validScheme = \str_starts_with($dsn, 'predis:');

        if (!$validScheme) {
            throw new InvalidArgumentException('Invalid dsn scheme type. Expected predis');
        }

        $url = \str_replace('predis:', 'file:', $dsn);

        if (false === $params = parse_url($url)) {
            throw new InvalidArgumentException('The given Predis DSN is invalid.');
        }

        if (isset($params['query'])) {
            parse_str($params['query'], $options);

            if (isset($options['username'])) {
                $options['username'] = rawurldecode($options['username']);
            }

            if (isset($options['password'])) {
                $options['password'] = rawurldecode($options['password']);
            }
        }

        $options['dsn_list'] = $params['host'] . ':' . $params['port'];

        return new self($options);
    }

    private function claimOldPendingMessages(): void
    {
        try {
            $command = $this->redis->createCommand('XPENDING', [$this->stream, $this->group, '-', '+', '1']);

            $commandResult = $this->redis->executeCommand($command);

            $pendingMessages = $commandResult ?: [];
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
                        'XCLAIM',
                        [
                            $this->stream,
                            $this->group,
                            $this->consumer,
                            $this->redeliverTimeout,
                            ...$claimableIds,
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

        $queuedMessageCount = (int) ($this->redis->zcount($this->queue, 0, $now) ?? 0);

        while ($queuedMessageCount--) {
            if (!$message = $this->redis->zpopmin($this->queue, 1)) {
                break;
            }

            $queuedMessage = array_key_first($message);
            $expiry = $message[$queuedMessage];

            if (\strlen($expiry) === \strlen($now) ? $expiry > $now : \strlen($expiry) < \strlen($now)) {
                // if a future-placed message is popped because of a race condition with
                // another running consumer, the message is readded to the queue

                $command = $this->redis->createCommand('ZADD', [$this->queue, 'NX', $expiry, $queuedMessage]);

                if (!$this->redis->executeCommand($command)) {
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
            $command = $this->redis->createCommand(
                'XREADGROUP',
                [
                    'GROUP',
                    $this->group,
                    $this->consumer,
                    'COUNT',
                    1,
                    'BLOCK',
                    1,
                    'STREAMS',
                    $this->stream,
                    $messageId,
                ],
            );

            $messages = $this->redis->executeCommand($command);

            if (is_array($messages)) {
                $messages = $this->reformatMessages($messages);
            }
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

    private function reformatMessages(array $messages): array
    {
        $result = [];

        foreach ($messages as $message) {
            if (empty($message[1])) {
                continue;
            }

            $result[$message[0]][$message[1][0][0]] = [$message[1][0][1][0] => $message[1][0][1][1]];
        }

        return $result;
    }

    public function ack(string $id): void
    {
        try {
            $acknowledged = $this->redis->executeCommand(
                $this->redis->createCommand(
                    'XACK',
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
                    'XACK',
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
                $now[0] = str_pad((string) ($delayInMs + (int) substr($now[0], 2, 3)), 3, '0', \STR_PAD_LEFT);
                if (3 < \strlen($now[0])) {
                    $now[1] += (int) substr($now[0], 0, -3);
                    $now[0] = substr($now[0], -3);

                    if (\is_float($now[1])) {
                        throw new TransportException("Message delay is too big: {$delayInMs}ms.");
                    }
                }
                $command = $this->redis->createCommand('ZADD', [$this->queue, 'NX', $now[1] . $now[0], $message]);

                $added = $this->redis->executeCommand($command);
            } else {
                $message = json_encode([
                    'body' => $body,
                    'headers' => $headers,
                ]);

                if (false === $message) {
                    throw new TransportException(json_last_error_msg());
                }

                if ($this->maxEntries) {
                    $added = $this->redis->xadd($this->stream, ['message' => $message], '*', ['limit' => $this->maxEntries, 'trim' => true]);
                } else {
                    $added = $this->redis->xadd($this->stream, ['message' => $message], '*');
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
            $this->redis->executeCommand($this->redis->createCommand('XGROUP', ['CREATE', $this->stream, $this->group, 0, 'MKSTREAM']));
        } catch (PredisException $e) {
            if (!\str_starts_with($e->getMessage(), 'BUSYGROUP')) {
                throw new TransportException($e->getMessage(), 0, $e);
            }
        }

        if ($this->deleteAfterAck || $this->deleteAfterReject) {
            $groups = $this->redis->executeCommand(
                $this->redis->createCommand(
                    'XINFO',
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
                throw new LogicException(
                    sprintf(
                        'More than one group exists for stream "%s", delete_after_ack and delete_after_reject cannot be enabled as it risks deleting messages before all groups could consume them.',
                        $this->stream
                    )
                );
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
                            'UNLINK',
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
        $command = $this->redis->createCommand('XINFO', ['GROUPS', $this->stream]);

        $groups = $this->redis->executeCommand($command) ?: [];

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
}