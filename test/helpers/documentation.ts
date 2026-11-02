import assert from 'node:assert/strict';
import { access, readFile, readdir } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';

/** Validate the standalone package's guides/examples rather than references to a consumer checkout. */
export async function checkDocumentation(root: string): Promise<void> {
  const pages = [
    'README.md',
    'CHANGELOG.md',
    'examples/README.md',
    ...(await readdir(join(root, 'docs'))).map((name) => 'docs/' + name),
  ];
  for (const page of pages) {
    const body = await readFile(join(root, page), 'utf8');
    for (const match of body.matchAll(/\[[^\]]*\]\(([^\s)]+)\)/g)) {
      const target = match[1]!;
      if (/^[a-z][a-z0-9+.-]*:/i.test(target)) continue;
      const [file, anchor] = target.split('#');
      const path = resolve(root, dirname(page), file || page.split('/').at(-1)!);
      await access(path);
      if (anchor) {
        const markdown = await readFile(path, 'utf8');
        const headings = [...markdown.matchAll(/^#{1,6}\s+(.+)$/gm)].map((heading) =>
          heading[1]!
            .toLowerCase()
            .replace(/[^\p{L}\p{N}_\s-]/gu, '')
            .replace(/\s/g, '-'),
        );
        assert.ok(
          headings.includes(decodeURIComponent(anchor)),
          `${page}: missing heading ${target}`,
        );
      }
    }
  }
}
