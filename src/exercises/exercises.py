"""Exercises: ORM fundamentals.

Implement the TODO functions. Autograder will test them.
"""

from __future__ import annotations

from typing import Optional

import assignment
from flask import request
from multiprocessing.reduction import duplicate
from sqlalchemy.exc import IntegrityError
from sqlalchemy import func

from src.exercises.extensions import db
from src.exercises.models import Student, Grade, Assignment


# ===== BASIC CRUD =====

def create_student(name: str, email: str) -> Student:
    """TODO: Create and commit a Student; handle duplicate email.

    If email is duplicate:
      - rollback
      - raise ValueError("duplicate email")
    """
    student = Student(name=name, email=email)
    db.session.add(student)
    try:
        db.session.commit()
        return student
    except IntegrityError:
        db.session.rollback()
        raise ValueError("duplicate email")


def find_student_by_email(email: str) -> Optional[Student]:
    """TODO: Return Student by email or None."""
    student = db.session.query(Student).filter(Student.email == email).one_or_none()
    return student


def add_grade(student_id: int, assignment_id: int, score: int) -> Grade:
    """TODO: Add a Grade for the student+assignment and commit.

    If student doesn't exist: raise LookupError
    If assignment doesn't exist: raise LookupError
    If duplicate grade: raise ValueError("duplicate grade")
    """
    student = db.session.get(Student, student_id)
    if not student:
        raise LookupError("student not found")
    assignment = db.session.get(Assignment, assignment_id)
    if not assignment:
        raise LookupError("assignment not found")
    grade = Grade(student_id=student_id, assignment_id=assignment_id, score = score)
    db.session.add(grade)
    try:
        db.session.commit()
        return grade
    except IntegrityError:
        raise ValueError("duplicate grade")


def average_percent(student_id: int) -> float:
    """TODO: Return student's average percent across assignments.

    percent per grade = score / assignment.max_points * 100

    If student doesn't exist: raise LookupError
    If student has no grades: return 0.0
    """
    student = db.session.get(Student, student_id)
    if not student:
        raise LookupError
    grades = db.session.query(Grade).filter(Grade.student_id==student_id)
    if grades.count() == 0:
        return 0.0
    sum =0
    numGrades =0
    for grade in grades:
        assgnmnt = db.session.get(Assignment, grade.assignment_id)
        percent_grade = grade.score / assgnmnt.max_points * 100
        sum += percent_grade
        numGrades +=1
    return sum / numGrades



# ===== QUERYING & FILTERING =====

def get_all_students() -> list[Student]:
    """TODO: Return all students in database, ordered by name."""
    return db.session.query(Student).order_by(Student.name).all()


def get_assignment_by_title(title: str) -> Optional[Assignment]:
    """TODO: Return assignment by title or None."""
    return db.session.query(Assignment).filter(Assignment.title==title).one_or_none()


def get_student_grades(student_id: int) -> list[Grade]:
    """TODO: Return all grades for a student, ordered by assignment title.

    If student doesn't exist: raise LookupError
    """
    student = db.session.get(Student , student_id)
    if not student:
        raise LookupError
    return (
        Grade.query.join(Assignment).filter(Grade.student_id==student_id)
        .order_by(Assignment.title).all()
    )

def get_grades_for_assignment(assignment_id: int) -> list[Grade]:
    """TODO: Return all grades for an assignment, ordered by student name.

    If assignment doesn't exist: raise LookupError
    """
    assignment = db.session.get(Assignment, assignment_id)
    if not assignment:
        raise LookupError
    return (
        db.session.query(Grade).join(Assignment)
        .filter(Grade.assignment_id==assignment_id)
        .join(Student).filter(Grade.student_id==Student.id)
        .order_by(Student.name).all()
    )

# ===== AGGREGATION =====

def total_student_grade_count() -> int:
    """TODO: Return total number of grades in database."""
    return  db.session.query(Grade).count()


def highest_score_on_assignment(assignment_id: int) -> Optional[int]:
    """TODO: Return the highest score on an assignment, or None if no grades.

    If assignment doesn't exist: raise LookupError
    """
    assignment = db.session.get(Assignment, assignment_id)
    if not assignment:
        raise LookupError
    grade=   db.session.query(Grade).filter(Grade.assignment_id==assignment_id).order_by(Grade.score.desc()).first()
    if grade:
        return grade.score
    return None
def class_average_percent() -> float:
    """TODO: Return average percent across all students and all assignments.

    percent per grade = score / assignment.max_points * 100
    Return average of all these percents.
    If no grades: return 0.0
    """
    expr = func.avg(Grade.score * 100.0/Assignment.max_points)
    result = (
        db.session.query(expr).select_from(Grade).join(Assignment, Grade.assignment_id==Assignment.id)
        .scalar()
    )
    return float(result) if result is not None else 0.0


def student_grade_count(student_id: int) -> int:
    """TODO: Return number of grades for a student.

    If student doesn't exist: raise LookupError
    """
    student = db.session.get(Student, student_id)
    if not student:
        raise LookupError
    return db.session.query(Grade).filter_by(student_id=student_id).count()

# ===== UPDATING & DELETION =====

def update_student_email(student_id: int, new_email: str) -> Student:
    """TODO: Update a student's email and commit.

    If student doesn't exist: raise LookupError
    If new email is duplicate: rollback and raise ValueError("duplicate email")
    Return the updated student.
    """
    dup = db.session.query(Student).filter(Student.email==new_email).one_or_none()
    if dup:
        raise ValueError("duplicate email")
    student = db.session.get(Student, student_id)
    if not student:
        raise LookupError
    student.email = new_email
    try:
        return student
    except Exception as e:
        db.session.rollback()
        raise e



def delete_student(student_id: int) -> None:
    """TODO: Delete a student and all their grades; commit.

    If student doesn't exist: raise LookupError
    """
    student = db.session.get(Student, student_id)
    if not student:
        raise LookupError
    db.session.delete(student)
    db.session.commit()

def delete_grade(grade_id: int) -> None:
    """TODO: Delete a grade by id; commit.

    If grade doesn't exist: raise LookupError
    """
    grade = db.session.get(Grade, grade_id)
    if not grade:
        raise LookupError
    db.session.delete(grade)
    db.session.commit()


# ===== FILTERING & FILTERING WITH AGGREGATION =====

def students_with_average_above(threshold: float) -> list[Student]:
    """TODO: Return students whose average percent is above threshold.

    List should be ordered by average percent descending.
    percent per grade = score / assignment.max_points * 100
    """
    expr = Grade.score*100/Assignment.max_points
    return ( db.session.query(Student).join(Grade).join(Assignment).group_by(Student.id)
    .having(func.avg(expr)>threshold)
           .order_by(func.avg(expr).desc()).all())


def assignments_without_grades() -> list[Assignment]:
    """TODO: Return assignments that have no grades yet, ordered by title."""
    return (db.session.query(Assignment).outerjoin(Grade)
                   .filter(Grade.id==None).order_by(Assignment.title).all())


def top_scorer_on_assignment(assignment_id: int) -> Optional[Student]:
    """TODO: Return the Student with the highest score on an assignment.

    If assignment doesn't exist: raise LookupError
    If no grades on assignment: return None
    If tie (multiple students with same high score): return any one
    """
    assignment = db.session.get(Assignment, assignment_id)
    if not assignment:
        raise LookupError
    grade = (db.session.query(Grade)
             .filter(Grade.assignment_id==assignment_id).order_by(Grade.score.desc())
             .first())
    if not grade:
        return None
    return grade.student
