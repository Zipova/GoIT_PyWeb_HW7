from sqlalchemy import func, desc, select, and_

from database.models import Teacher, Student, Discipline, Grade, Group
from database.db import session


def select_one():
    # Знайти 5 студентів із найбільшим середнім балом з усіх предметів.
    result = session.query(Student.fullname, func.round(func.avg(Grade.grade), 2).label('avg_grade'))\
            .select_from(Grade).join(Student).group_by(Student.id).order_by(desc('avg_grade')).limit(5).all()
    return result


def select_two(discipline_id):
    '''
    Знайти студента із найвищим середнім балом з певного предмета.

    SELECT AVG(g.grade) as average_grade, s.fullname
    FROM grades as g
    JOIN students AS s ON s.id = g.student_id
    WHERE discipline_id = 6
    GROUP BY s.fullname
    ORDER BY average_grade DESC
    LIMIT 1;
    '''
    result = session.query(Discipline.name, Student.fullname, func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade).join(Student).join(Discipline)\
        .filter(Discipline.id == discipline_id)\
        .group_by(Student.id, Discipline.name).order_by(desc('avg_grade')).limit(1).first()
    return result


def select_three(discipline_id):
    '''
    Знайти середній бал у групах з певного предмета.

    SELECT AVG(g.grade) as average_grade, gr.name
    FROM grades as g
    JOIN students AS s ON s.id = g.student_id
    JOIN groups AS gr ON s.group_id = gr.id
    WHERE g.discipline_id = 1
    GROUP BY gr.name
    '''

    result = session.query(Group.name, func.round(func.avg(Grade.grade), 2).label('avg_grade'))\
        .select_from(Grade).join(Student).join(Group)\
        .filter(Grade.discipline_id == discipline_id)\
        .group_by(Group.name).all()
    return result

def select_four():
    '''
    Знайти середній бал на потоці (по всій таблиці оцінок).

    SELECT AVG(g.grade) as average_grade
    FROM grades as g
    '''

    result = session.query(func.round(func.avg(Grade.grade), 2).label('avg_grade'))\
        .select_from(Grade).all()
    return result


def select_five(teacher_id):
    '''
    Знайти які курси читає певний викладач.

    SELECT t.fullname, d.name
    FROM teachers AS t
    JOIN disciplines AS d ON d.teacher_id = t.id
    WHERE teacher_id = 2

    '''
    result = session.query(Teacher.fullname, Discipline.name)\
        .select_from(Teacher).join(Discipline)\
        .filter(Teacher.id == teacher_id).all()
    return result


def select_six(group_id):
    '''
    Знайти список студентів у певній групі.

    SELECT gr.name, s.fullname
    FROM students AS s
    JOIN groups AS gr ON s.group_id = gr.id
    WHERE gr.id = 1
    :return:
    '''
    result = session.query(Group.name, Student.fullname)\
        .select_from(Student).join(Group).filter(Group.id == group_id).all()
    return result


def select_seven(group_id, discipline_id):
    '''
    Знайти оцінки студентів у окремій групі з певного предмета.

    SELECT g.grade, s.fullname
    FROM grades as g
    JOIN students AS s ON s.id = g.student_id
    WHERE s.group_id = 1 AND g.discipline_id = 1

    '''

    result = session.query(Grade.grade, Student.fullname)\
        .select_from(Grade).join(Student)\
        .filter(and_(Student.group_id == group_id, Grade.discipline_id == discipline_id)).all()
    return result


def select_eight(teacher_id):
    '''
    Знайти середній бал, який ставить певний викладач зі своїх предметів.

    SELECT AVG(g.grade), d.name
    FROM grades as g
    JOIN disciplines AS d ON d.id = g.discipline_id
    WHERE d.teacher_id = 4
    GROUP BY d.name
    :return:
    '''
    result = session.query(Discipline.name, func.round(func.avg(Grade.grade), 2).label('avg_grade'))\
        .select_from(Grade).join(Discipline)\
        .filter(Discipline.teacher_id == teacher_id).group_by(Discipline.name).all()
    return result


def select_nine(student_id):
    '''
    Знайти список курсів, які відвідує студент.

    SELECT d.name
    FROM grades as g
    JOIN disciplines AS d ON d.id = g.discipline_id
    WHERE g.student_id = 25
    GROUP BY d.name
    :return:
    '''
    result = session.query(Discipline.name)\
        .select_from(Grade).join(Discipline)\
        .filter(Grade.student_id == student_id)\
        .group_by(Discipline.name).all()
    return result


def select_ten(student_id, teacher_id):
    '''
    Список курсів, які певному студенту читає певний викладач.

    SELECT d.name
    FROM grades as g
    JOIN disciplines AS d ON d.id = g.discipline_id
    WHERE g.student_id = 29 AND d.teacher_id = 3
    GROUP BY d.name
    :return:
    '''
    result = session.query(Discipline.name)\
        .select_from(Grade).join(Discipline)\
        .filter(and_(Grade.student_id == student_id, Discipline.teacher_id == teacher_id))\
        .group_by(Discipline.name).all()
    return result


def select_eleven(student_id, teacher_id):
    '''
    Середній бал, який певний викладач ставить певному студентові.

    SELECT AVG(g.grade) as average_grade
    FROM grades as g
    JOIN disciplines AS d ON d.id = g.discipline_id
    WHERE g.student_id = 16 AND d.teacher_id = 2
    :return:
    '''
    result = session.query(func.round(func.avg(Grade.grade), 2).label('avg_grade'))\
        .select_from(Grade).join(Discipline)\
        .filter(and_(Grade.student_id == student_id, Discipline.teacher_id == teacher_id)).all()
    return result


def select_twelve(group_id, discipline_id):
    '''
    Оцінки студентів у певній групі з певного предмета на останньому занятті.

    SELECT g.grade, s.fullname, g.date_of
    FROM grades as g
    JOIN students AS s ON s.id = g.student_id
    WHERE g.discipline_id = 4 AND s.group_id = 2 AND g.date_of IN (SELECT MAX(date_of)
                                                                    FROM grades)
    :return:
    '''
    subquery = (select(func.max(Grade.date_of)).join(Student).filter(and_(
        Grade.discipline_id == discipline_id, Student.group_id == group_id
        )).scalar_subquery())
    result = session.query(Grade.grade, Student.fullname, Grade.date_of)\
        .select_from(Grade).join(Student)\
        .filter(and_(Grade.discipline_id == discipline_id, Student.group_id == group_id, Grade.date_of == subquery)).all()
    return result


if __name__ == '__main__':
    print("Знайти 5 студентів із найбільшим середнім балом з усіх предметів.\n", select_one())
    print("Знайти студента із найвищим середнім балом з певного предмета.\n", select_two(6))
    print("Знайти середній бал у групах з певного предмета.\n", select_three(1))
    print("Знайти середній бал на потоці (по всій таблиці оцінок).\n", select_four())
    print("Знайти які курси читає певний викладач.\n", select_five(2))
    print("Знайти список студентів у певній групі.\n", select_six(2))
    print("Знайти оцінки студентів у окремій групі з певного предмета.\n", select_seven(1, 3))
    print("Знайти середній бал, який ставить певний викладач зі своїх предметів.\n", select_eight(3))
    print("Знайти список курсів, які відвідує певний студент.\n", select_nine(25))
    print("Список курсів, які певному студенту читає певний викладач.\n", select_ten(14, 3))
    print("Середній бал, який певний викладач ставить певному студентові\n", select_eleven(11, 2))
    print("Оцінки студентів у певній групі з певного предмета на останньому занятті.\n", select_twelve(1, 3))






