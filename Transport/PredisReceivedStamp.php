<?php

/*
 * This file is part of the Symfony package.
 *
 * (c) Aleksandr Kaluzhskii <nyatmeat@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Symfony\Component\Messenger\Bridge\Predis\Transport;

use Symfony\Component\Messenger\Stamp\NonSendableStampInterface;

/**
 * @author Aleksandr Kaluzhskii <nyatmeat@gmail.com>
 */
class PredisReceivedStamp implements NonSendableStampInterface
{
    public function __construct(
        private string $id,
    ) {
    }

    public function getId(): string
    {
        return $this->id;
    }
}
