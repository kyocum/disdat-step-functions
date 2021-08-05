echo "Building Disdat-step-function package locally"
rm -rf disdat_step_function.egg-info
rm -rf dist

python3 -m build

echo "upload to TestPyPi"
python3 -m twine upload --repository testpypi dist/* --skip-existing

echo "Create a new venv for testing"
rm -rf .testenv
python3 -m venv .testenv
source .testenv/bin/activate
pip install -i https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple disdat-step-function
#
python3 -c "
from  disdat_step_function.caching_wrapper import Caching
print('Test Done')"

deactivate
echo "Clean up testing env"
rm -rf .testenv


if false;  then
  echo "upload to PyPi for real!"
fi