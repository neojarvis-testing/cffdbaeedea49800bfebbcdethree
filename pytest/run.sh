# Copy the test file
cp /home/coder/project/workspace/pytest/tests.py /home/coder/project/workspace/project/tests.py

# Navigate to the project directory
cd /home/coder/project/workspace/project

# Install dependencies
pip3 install -r requirements.txt

# Run the tests
python3 -m pytest tests.py -v
