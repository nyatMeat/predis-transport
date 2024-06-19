<?php

declare(strict_types=1);

namespace Symfony\Component\Messenger\Bridge\Predis\Transport\Predis;

use Predis\Command\Command;

/**
 * @see https://redis.io/docs/latest/commands/xack/
 */
class XACK extends Command
{
    public function getId()
    {
        return 'XACK';
    }
}