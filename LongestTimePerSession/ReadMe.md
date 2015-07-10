#IIS Log Timing Report
Takes an IIS log and calculates the longest time waiting for a single call and the total time
per session.

I have also added several refactors, to make the code more generic.

To modify the report, consider only changing the global strings at the beginning of the file and the 
  ReportSectionFunctions class.