# Contributing

## License

By contributing to this project, you agree that your contributions will be licensed under the project's license (MIT OR Apache-2.0).

Thank you for your contribution!

## Looking for issues?

https://github.com/Gifted-s/bd/issues


### How do I contribute

Fork the project and use the `git clone` command to download the repository to your computer. A standard procedure for working on an issue would be to:

1. Clone the `velarixdb` repository and download it to your computer.
    ```bash
    git clone https://github.com/Gifted-s/velarixdb
    ```

2. Pull all changes from the upstream `main` branch, before creating a new branch - to ensure that your `main` branch is up-to-date with the latest changes:
    ```bash
    git pull
    ```

3. Create a new branch from `main` like: `bugfix-232-ensure-compaction-runs-within-time-frame-allocated`:
    ```bash
    git checkout -b "[the name of your branch]"
    ```

4. Make changes to the code, and ensure all code changes are formatted correctly:
    ```bash
    cargo fmt
    ```
5. Ensure all clippy rules are obeyed:
    ```bash
    cargo clippy --tests
    ```

6. Ensure nothing is unexpectedly broken:
    ```bash
    cargo test
    ```

6. Commit your changes when finished:
    ```bash
    git add -A
    git commit -m "[your commit message]"
    ```

7. Push changes to GitHub:
    ```bash
    git push origin "[the name of your branch]"
    ```

8. Submit your changes for review, by going to your repository on GitHub and clicking the `Compare & pull request` button.

9. Ensure that you have entered a commit message which details the changes, and what the pull request is for.

10. Now submit the pull request by clicking the `Create pull request` button.

11. Wait for code review and approval.

12. After approval, your pull request will be merged.