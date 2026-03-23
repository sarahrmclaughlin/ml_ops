import pandas as pd

# from evidently.report import Report
# from evidetly.metrics_preset import DataDriftPreset


# Define the default arguments for the DAG

data = pd.DataFrame(
    {
        "age": [1, 2, 3, 4, 5],
        "weight": [2, 3, 4, 5, 6],
    }
)
data.head()
# report = Report(metrics=[DataDriftPreset()])
# report.run(reference_data=data, current_data=data)

# report.save('example1.html')
