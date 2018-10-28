def month_letters_to_numbers(argument):
    switcher = {
        'Jan': '01',
        'Feb': '02',
        'Mar': '03',
        'Apr': '04',
        'May': '05',
        'Jun': '06',
        'Jul': '07',
        'Aug': '08',
        'Sep': '09',
        'Oct': '10',
        'Nov': '11',
        'Dec': '12'
    }
    return switcher.get(argument)


with open(r'C:\workspace-intellij\neural-network-thesis\src\main\resources\BTC_USD_raw.csv', 'r') as infile, \
        open(r'C:\workspace-intellij\neural-network-thesis\src\main\resources\BTC_USD_train.csv', 'w') as outfile1, \
        open(r'C:\workspace-intellij\neural-network-thesis\src\main\resources\BTC_USD_test.csv', 'w') as outfile2:

    # remove line with column names
    lines = infile.readlines()
    lines = lines[1:]

    for line in lines:
        # remove some non-digits
        line = line.replace('%', '')
        line = line.replace('K', '')

        # remove first and last quote character
        end = len(line) - 2
        line = line[1:end] + "\n"

        # remove comma from date
        tempList = line.split(',', 1)
        line = tempList[0] + tempList[1]

        # change month letters to numbers
        monthNum = month_letters_to_numbers(line[:3])
        line = monthNum + line[3:]

        # remove commas from numbers and quote characters
        tempList = line.split('"')
        tempList2 = ['']

        for s in tempList:
            s = s.replace(',', '')
            if s != '':
                tempList2.append(s)

        line = ','.join(tempList2)
        line = line[1:len(line)]

        date = line.split(',', 1)[0]
        if "2018" not in date:
            outfile1.write(line)
        else:
            outfile2.write(line)

