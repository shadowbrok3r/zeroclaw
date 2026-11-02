import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
OFFICIAL = "github.repository == 'zeroclaw-labs/zeroclaw'"


def workflow(name):
    return (ROOT / ".github/workflows" / name).read_text(encoding="utf-8")


def jobs(name):
    body = workflow(name).split("\njobs:\n", 1)[1]
    headers = list(re.finditer(r"(?m)^  ([a-z0-9-]+):$", body))
    result = {}
    for index, header in enumerate(headers):
        end = headers[index + 1].start() if index + 1 < len(headers) else len(body)
        result[header[1]] = body[header.end():end]
    return result


class ForkWorkflowContract(unittest.TestCase):
    def assert_official_guard(self, body):
        condition = re.search(r"(?m)^    if: (.+)$", body)
        self.assertIsNotNone(condition)
        value = condition[1].removeprefix("${{ ").removesuffix(" }}")
        self.assertTrue(value == OFFICIAL or value.startswith(OFFICIAL + " && "), value)

    def test_public_mutation_jobs_require_the_official_repository(self):
        guarded = {
            "daily-audit.yml": ["advisories"],
            "daily-npm-audit.yml": ["npm-audit"],
            "monthly-outdated.yml": ["outdated"],
            "docs-deploy.yml": ["deploy", "promote-stable"],
            "pub-crates.yml": ["publish"],
            "pub-aur.yml": ["publish-aur"],
            "pub-scoop.yml": ["publish-scoop"],
            "scoop-bucket-canary.yml": ["latest-release"],
            "discord-release.yml": ["discord"],
            "tweet-release.yml": ["tweet"],
        }
        for name, names in guarded.items():
            for job in names:
                with self.subTest(workflow=name, job=job):
                    self.assert_official_guard(jobs(name)[job])
        for job, body in jobs("release-stable-manual.yml").items():
            if not re.search(r"(?m)^    needs:", body):
                with self.subTest(workflow="release-stable-manual.yml", job=job):
                    self.assert_official_guard(body)

    def test_fork_validation_keeps_read_only_pr_events(self):
        for name in ["ci.yml", "windows-tests.yml"]:
            header = workflow(name).split("\njobs:\n", 1)[0]
            with self.subTest(workflow=name):
                self.assertIn("branches: [main, master]", header)
                self.assertIn("permissions:\n  contents: read\n", header)
                self.assertNotIn("pull_request_target:", header)
                self.assertNotIn("secrets:", workflow(name))
                self.assertNotIn("${{ secrets.", workflow(name))
        self.assertIsNone(re.search(r"(?m)^    if:", jobs("pub-crates.yml")["preflight"]))

    def test_advisory_scanners_use_the_verified_installer(self):
        for name in ["ci.yml", "daily-audit.yml"]:
            text = workflow(name)
            for tool in ["cargo-deny", "cargo-audit"]:
                with self.subTest(workflow=name, tool=tool):
                    remainder = text.split(f"      - name: Install {tool}\n", 1)[1]
                    step = remainder.split("\n      - ", 1)[0]
                    self.assertIn(f"run: bash scripts/ci/install_release_tool.sh {tool}", step)
                    self.assertNotIn("curl ", step)


if __name__ == "__main__":
    unittest.main()
