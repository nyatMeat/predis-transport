<?php

declare(strict_types=1);

namespace Symfony\Component\Messenger\Bridge\Predis\Tests\Transport\Predis;

use Predis\Command\Command;

class XREADGROUP extends Command
{
    public function getId()
    {
        return 'XREADGROUP';
    }
}