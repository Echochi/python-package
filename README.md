# python-package
Learning to make a python package

## Testing
_Notes on setting up testing in VSCode_
### 1. Enable python unittests in Workspace settings, e.g.
```javascript
{
    "python.pythonPath": "${workspaceFolder}/bin/python3",
    // python.unitTest.unittestArgs are actually just the default here.
    "python.unitTest.unittestArgs": [
        "-v",
        "-s",
        ".",
        "-p",
        "*test*.py"
    ],
    "python.unitTest.unittestEnabled": true,
}
```

### 2. Make tests discoverable
Add `__init__.py` so that test can be found in packages (subfolders)
Make sure your test file has the word `test` in it.

### 3. Running tests
Tests are run automatically on commit. Check the output tab (`CMD+SHIFT+U`)
Or, open your test file and `Run Test` manually.