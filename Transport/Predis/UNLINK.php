<?php

declare(strict_types=1);

namespace Symfony\Component\Messenger\Bridge\Predis\Transport\Predis;

use Predis\Command\Command;

/**
 * @see
 */
class UNLINK extends Command
{
    public function getId()
    {
        return 'UNLINK';
    }
}