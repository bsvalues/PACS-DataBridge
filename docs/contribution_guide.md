
# PACS DataBridge Contribution Guide

This document outlines how to contribute to the PACS DataBridge project and track your progress.

## Getting Started

1. Clone the repository
2. Install dependencies using `pip install -r requirements.txt`
3. Familiarize yourself with the codebase structure
4. Check the `docs/project_tracker.md` for current status and pending tasks

## Development Workflow

1. **Choose a Task**: Select an open task from the project tracker
2. **Update Status**: Mark the task as "In Progress" in the tracker and add your name
3. **Create Branch**: Create a feature branch with the format `feature/brief-description`
4. **Implement**: Write code and tests for your feature
5. **Test**: Run tests to ensure everything works correctly
6. **Document**: Update relevant documentation
7. **Pull Request**: Submit a PR with a reference to the task ID

## Running Components

### CLI Tool
```
python src/databridge.py cli <command>
```

### Web API
```
python src/databridge.py api --host 0.0.0.0 --port 8000
```

### Web UI
```
python src/databridge.py web-ui --host 0.0.0.0 --port 8501
```

## Project Structure

- `src/data_bridge/` - Core modules
- `tests/` - Test suite
- `docs/` - Documentation
- `config/` - Configuration files

## Code Guidelines

1. Follow PEP 8 style guide for Python code
2. Write docstrings for all functions, classes, and modules
3. Include type hints where applicable
4. Ensure all new features have corresponding tests
5. Keep functions small and focused on a single responsibility

## Testing

Run tests using pytest:
```
pytest
```

## Updating the Project Tracker

After completing a task or making significant progress:

1. Open `docs/project_tracker.md`
2. Update the relevant task's status
3. Update component progress percentages if applicable
4. Add entries to the weekly status update section
5. Commit the updated tracker with your changes

## Weekly Updates

Each week, provide a status update in the tracker including:
- Tasks completed
- Tasks in progress
- Any blockers or issues
- Plans for next week

## Issue Reporting

If you encounter issues:

1. Check if the issue is already documented
2. Add to "Issue Tracking" section with a new ID
3. Include relevant details (priority, status, description)

## Release Process

1. Update version in `src/data_bridge/__init__.py`
2. Update CHANGELOG.md
3. Update documentation
4. Tag the release in git
5. Update the project tracker

## Questions?

If you have any questions about the contribution process, please reach out to the project manager.
