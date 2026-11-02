"""Claude Code implements a change, a fresh Codex worker reviews it, Claude fixes.

    ccx controller run examples/controller_implement_review.py "Add a --dry-run flag to sync.py"

Run it from a clean Git working tree: the reviewer looks at all uncommitted
changes. The reviewer returns its findings as an `issues` variable, most
significant first, and Claude marks each one fixed or rejected in the same list,
so nothing parses prose. "Don't edit" and "don't commit" are instructions, not
enforced.
"""
import sys

from ccx import WorkerClient, controller, current, task

REVIEWS = 2  # at most 2 reviews and 1 fix round
workers = WorkerClient()


@task
def implement(request: str):
    created = workers.create(engine="claude", cwd=str(current.workspace), name="implementer",
                             wait=True, prompt=(
        f"Implement this request: {request}\nKeep the diff small, add or update tests "
        "for changed behavior, run the relevant checks and report the command and result. "
        "Don't commit."))
    return created["worker"]


@task
def review(request: str):
    shared = {"issues": None}
    workers.create(engine="codex", cwd=str(current.workspace), name="reviewer",
                   wait=True, variables=shared, prompt=(
        f"Review the uncommitted changes in this repository (including new files) for this "
        f"request: {request}\nDon't edit any files. Look for problems the change introduced "
        "that you can demonstrate: wrong behavior, crashes, security or data loss. Skip style, "
        "minor issues and pre-existing problems. Your result goes in the shared `issues` "
        "variable, not in your reply: run python3 to set `issues` to a list of "
        "{'severity': 'high' or 'medium', 'where': 'path:line', 'problem': ...}, most "
        "significant first, or to [] if there are none."))
    return shared["issues"]


@task
def fix(author: str, issues: list):
    shared = {"issues": issues}
    workers.send(author, (
        "A reviewer listed problems in the `issues` variable, most significant first. With "
        "python3, read it. Fix the real ones with the smallest change, and set each issue's "
        "'status' to 'fixed' or 'rejected' with a short 'note'. Rerun the checks."),
        variables=shared)
    return shared["issues"]


@controller(name="implement-review")
def main():
    request = " ".join(sys.argv[1:])
    author = implement(request)
    for n in range(1, REVIEWS + 1):
        issues = review(request)
        if issues == []:
            current.report(status="completed", summary=f"No issues found (review {n}).",
                           data={"author": author})
            return
        if issues is None or n == REVIEWS:
            break
        fix(author, issues)
    current.report(status="needs-review", summary="The review didn't come back clean.",
                   data={"author": author, "issues": issues})


if __name__ == "__main__":
    main()
