import argparse
import sys
from sqlalchemy.exc import SQLAlchemyError

from database.models import Teacher, Student, Discipline, Grade, Group
from database.db import session

parser = argparse.ArgumentParser(description='HW-07')
parser.add_argument('--action', '-a', help='Command: create, update, list, remove')
parser.add_argument('--model', '-m', help='Models: teacher, student, discipline, grade, group')
parser.add_argument('--id')
parser.add_argument('--name')
parser.add_argument('--grade')
parser.add_argument('--date_of')
parser.add_argument('--student_id')
parser.add_argument('--teacher_id')
parser.add_argument('--discipline_id')
parser.add_argument('--group_id')


arguments = parser.parse_args()
# print(arguments)
my_arg = vars(arguments)
# print(my_arg)

action = my_arg.get('action')
model = my_arg.get('model')


def main():
    if action == 'create':
        print(create_field())
    elif action == 'list':
        print(read())
    elif action == 'update':
        print(update_field())
    elif action == 'remove':
        print(delete_field())


def create_field():
    if model == 'Teacher':
        new_field = Teacher(fullname = my_arg.get('name'))
    elif model == 'Student':
        new_field = Student(fullname=my_arg.get('name'), group_id=my_arg.get('group_id'))
    elif model == 'Discipline':
        new_field = Discipline(name=my_arg.get('name'), teacher_id=my_arg.get('teacher_id'))
    elif model == 'Grade':
        new_field = Grade(grade=my_arg.get('grade'), date_of=my_arg.get('date_of'), student_id=my_arg.get('student_id'), discipline_id=my_arg.get('discipline_id'))
    elif model == 'Group':
        new_field = Group(name = my_arg.get('name'))
    else:
        return 'Unknown command'
    session.add(new_field)
    session.commit()
    session.close()
    return 'Done!'


def read():
    if model == 'Teacher':
        fields = session.query(Teacher).all()
        for field in fields:
            print(field.id, field.fullname)
    elif model == 'Student':
        fields = session.query(Student).all()
        for field in fields:
            print(field.id, field.fullname, field.group_id)
    elif model == 'Discipline':
        fields = session.query(Discipline).all()
        for field in fields:
            print(field.id, field.name, field.teacher_id)
    elif model == 'Grade':
        fields = session.query(Grade).all()
        for field in fields:
            print(field.id, field.grade, field.date_of, field.student_id, field.discipline_id)
    elif model == 'Group':
        fields = session.query(Group).all()
        for field in fields:
            print(field.id, field.name)
    else:
        return 'Unknown command'
    return 'Done!'


def update_field():
    if model == 'Teacher':
        field = session.query(Teacher).filter(Teacher.id == my_arg.get('id'))
        if field:
            field.update({'fullname': my_arg.get('name')})
    elif model == 'Student':
        field = session.query(Student).filter(Student.id == my_arg.get('id'))
        if field:
            field.update({'fullname': my_arg.get('name'), 'group_id' : my_arg.get('group_id')})
    elif model == 'Discipline':
        field = session.query(Discipline).filter(Discipline.id == my_arg.get('id'))
        if field:
            field.update({'name' : my_arg.get('name'), 'teacher_id' : my_arg.get('teacher_id')})
    elif model == 'Grade':
        field = session.query(Grade).filter(Grade.id == my_arg.get('id'))
        if field:
            field.update({'grade' : my_arg.get('grade'), 'date_of' : my_arg.get('date_of'), 'student_id' : my_arg.get('student_id'), 'discipline_id' : my_arg.get('discipline_id')})
    elif model == 'Group':
        field = session.query(Group).filter(Group.id == my_arg.get('id'))
        if field:
            field.update({'name': my_arg.get('name')})
    else:
        return 'Unknown command'
    session.commit()
    session.close()
    return field.first()


def delete_field():
    if model == 'Teacher':
        session.query(Teacher).filter(Teacher.id == my_arg.get('id')).delete()
    elif model == 'Student':
        session.query(Student).filter(Student.id == my_arg.get('id')).delete()
    elif model == 'Discipline':
        session.query(Discipline).filter(Discipline.id == my_arg.get('id')).delete()
    elif model == 'Grade':
        session.query(Grade).filter(Grade.id == my_arg.get('id')).delete()
    elif model == 'Group':
        session.query(Group).filter(Group.id == my_arg.get('id')).delete()
    else:
        return 'Unknown command'
    session.commit()
    session.close()
    return 'Done!'


if __name__ == '__main__':
    main()