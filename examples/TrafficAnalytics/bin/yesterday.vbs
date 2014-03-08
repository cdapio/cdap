dateYesterday = Date() - 1
WScript.Echo Day(dateYesterday) & "/" & MonthName(Month(CDate(dateYesterday)), true) & "/" & Year(dateYesterday)
