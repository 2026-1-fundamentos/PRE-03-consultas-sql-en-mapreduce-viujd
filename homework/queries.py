# pylint: disable=broad-exception-raised
# pylint: disable=import-error

<<<<<<< HEAD
import .mapreduce.run_mapreduce_job as run_mapreduce_job  # type: ignore
=======
import mapreduce.run_mapreduce_job as run_mapreduce_job  # type: ignore
>>>>>>> 3e46c49dd78d360f23515bb04f7b9d24f3c4c550

#
# Columns:
# total_bill, tip, sex, smoker, day, time, size
#


#
# SELECT *, tip/total_bill as tip_rate
# FROM tips;
#
def mapper_query_1(sequence):
    """Mapper"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            result.append((index, row.strip() + ",tip_rate"))
        else:
            row_values = row.strip().split(",")
            total_bill = float(row_values[0])
            tip = float(row_values[1])
            tip_rate = tip / total_bill
            result.append((index, row.strip() + "," + str(tip_rate)))
    return result


def reducer_query_1(sequence):
    """Reducer"""
    return sequence


#
# SELECT *
# FROM tips
# WHERE time = 'Dinner';
#
def mapper_query_2(sequence):
    """Mapper"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            result.append((index, row.strip()))
        else:
            row_values = row.strip().split(",")
            if row_values[5] == "Dinner":
                result.append((index, row.strip()))
    return result


def reducer_query_2(sequence):
    """Reducer"""
    return sequence


#
# SELECT *
# FROM tips
# WHERE time = 'Dinner' AND tip > 5.00;
#
def mapper_query_3(sequence):
    """Mapper"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            result.append((index, row.strip()))
        else:
            row_values = row.strip().split(",")
            if row_values[5] == "Dinner" and float(row_values[1]) > 5.00:
                result.append((index, row.strip()))
    return result


def reducer_query_3(sequence):
    """Reducer"""
    return sequence


#
# SELECT *
# FROM tips
# WHERE size >= 5 OR total_bill > 45;
#
def mapper_query_4(sequence):
    """Mapper"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            result.append((index, row.strip()))
        else:
            row_values = row.strip().split(",")
            if int(row_values[6]) >= 5 or float(row_values[0]) > 45:
                result.append((index, row.strip()))
    return result


def reducer_query_4(sequence):
    """Reducer"""
    return sequence


#
# SELECT sex, count(*)
# FROM tips
# GROUP BY sex;
#
def mapper_query_5(sequence):
    """Mapper"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            continue
        row_values = row.strip().split(",")
        result.append((row_values[2], 1))
    return result


def reducer_query_5(sequence):
    """Reducer"""
    counter = dict()
    for key, value in sequence:
        if key not in counter:
            counter[key] = 0
        counter[key] += value
    return list(counter.items())


#
# ??
# SELECT day, AVG(tip)
# FROM tips
# GROUP BY day;
<<<<<<< HEAD
#
=======

def mapper_query_6(sequence):
    """Mapper"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            continue
        row_values = row.strip().split(",")
        day = row_values[3]
        tip = float(row_values[1])
        result.append((day, tip))
    return result


def reducer_query_6(sequence):
    """Reducer"""
    totals = dict()
    counts = dict()
    for key, value in sequence:
        if key not in totals:
            totals[key] = 0.0
            counts[key] = 0
        totals[key] += value
        counts[key] += 1
    return [(day, totals[day] / counts[day]) for day in totals]
>>>>>>> 3e46c49dd78d360f23515bb04f7b9d24f3c4c550


#
# ORQUESTADOR:
#
def run():
    """Orquestador"""

    run_mapreduce_job(
        mapper=mapper_query_1,
        reducer=reducer_query_1,
        input_directory="files/input",
        output_directory="files/query_1",
    )

    run_mapreduce_job(
        mapper=mapper_query_2,
        reducer=reducer_query_2,
        input_directory="files/input",
        output_directory="files/query_2",
    )

    run_mapreduce_job(
        mapper=mapper_query_3,
        reducer=reducer_query_3,
        input_directory="files/input",
        output_directory="files/query_3",
    )

    run_mapreduce_job(
        mapper=mapper_query_4,
        reducer=reducer_query_4,
        input_directory="files/input",
        output_directory="files/query_4",
    )

    run_mapreduce_job(
        mapper=mapper_query_5,
        reducer=reducer_query_5,
        input_directory="files/input",
        output_directory="files/query_5",
    )
<<<<<<< HEAD
=======

    run_mapreduce_job(
    mapper=mapper_query_6,
    reducer=reducer_query_6,
    input_directory="files/input",
    output_directory="files/query_6",
    )
>>>>>>> 3e46c49dd78d360f23515bb04f7b9d24f3c4c550

if __name__ == "__main__":



<<<<<<< HEAD
    run()
=======
    run()
>>>>>>> 3e46c49dd78d360f23515bb04f7b9d24f3c4c550
