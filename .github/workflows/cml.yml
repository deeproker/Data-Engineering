name: mlops-exaple-tensorflow-regression
on:
  pull_request:
    branches: [ main ]
    paths:
      - 'Github-ML-pipeline-Demo/**'
jobs:
  run:
    runs-on: [ubuntu-latest]
    container: docker://dvcorg/cml-py3:latest
    steps:
      - uses: actions/checkout@v2
      - name: 'Train my model'
        env:
          repo_token: ${{ secrets.GITHUB_TOKEN }}
        run: |
          # Your ML workflow goes here
          cd ./"Github-ML-pipeline-Demo"
          echo "Changed Directory Successfully"
          pip install -r requirements.txt
          python model.py          
          echo "## Model Metrics" > report.md
          cat metrics.txt >> report.md          
          echo "\n## Model Performance" >> report.md
          echo "Model performance metrics are on the plot below." >> report.md          
          cml-publish model_results.png --md >> report.md          
          cml-send-comment report.md
