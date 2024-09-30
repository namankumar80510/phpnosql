# PHPNOSQL

A PHP based NoSQL database that stores data in PHP Arrays.

## Installation

```bash
composer require dikki/phpnosql
```

## Usage

See the PhpNoSql.php class to observe the methods once.

Look at this simple Blog Post class to observe how to use the PhpNoSql class.

```php
<?php

declare(strict_types=1);

use Dikki\PhpNoSql\PhpNoSql;

class BlogPost
{
    private PhpNoSql $db;

    public function __construct(PhpNoSql $db)
    {
        $this->db = $db;
    }

    public function createPost(string $title, string $content, string $author): string
    {
        $post = [
            'title' => $title,
            'content' => $content,
            'author' => $author,
            'created_at' => date('Y-m-d H:i:s'),
            'updated_at' => date('Y-m-d H:i:s'),
        ];

        return $this->db->create($post);
    }

    public function getPost(string $id): ?array
    {
        $posts = $this->db->read(['id' => $id]);
        return $posts[0] ?? null;
    }

    public function getAllPosts(): array
    {
        return $this->db->read([], ['order' => ['created_at' => 'DESC']]);
    }

    public function updatePost(string $id, string $title, string $content): bool
    {
        $updated = $this->db->update(['id' => $id], [
            'title' => $title,
            'content' => $content,
            'updated_at' => date('Y-m-d H:i:s'),
        ]);

        return $updated > 0;
    }

    public function deletePost(string $id): bool
    {
        return $this->db->delete(['id' => $id]) > 0;
    }
}
```

**Docs will be updated soon.**
