<?php

declare(strict_types=1);

namespace Bavix\LaravelClickHouse;

use Illuminate\Support\ServiceProvider;
use Illuminate\Database\DatabaseManager;
use Bavix\LaravelClickHouse\Database\Connection;
use Bavix\LaravelClickHouse\Database\Eloquent\Model;

class ClickHouseServiceProvider extends ServiceProvider
{
    /**
     * @return void
     */
    public function boot(): void
    {
        $this->app->resolving('db', function ($db) {
            $db->extend('clickhouse-ext', function ($config, $name) {
                $config['name'] = $name;
                $connection = new Connection($config);
                if ($this->app->bound('events')) {
                    $connection->setEventDispatcher($this->app['events']);
                }

                return $connection;
            });

            Model::setConnectionResolver($db);
        });
    }
}
