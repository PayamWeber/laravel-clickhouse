<?php

namespace Bavix\LaravelClickHouse\Database\Query;

use Bavix\LaravelClickHouse\Database\Query\Traits\JoinComponentCompiler;
use Tinderbox\ClickhouseBuilder\Query\Grammar;

class Grammarr extends Grammar
{
	use JoinComponentCompiler;
}