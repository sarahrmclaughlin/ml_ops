import pandas as pd
from evidently.report import Report
from evidetly.metrics_preset import DataDriftPreset

data = pd.DataFrame({
    'age': [1, 2, 3, 4, 5],
    'weight': [2, 3, 4, 5, 6],
})

report = Report(metrics=[DataDriftPreset()])
report.run(reference_data=data, current_data=data)

report.save('example1.html')