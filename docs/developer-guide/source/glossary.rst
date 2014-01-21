:orphan:

.. _glossary:

========
Glossary
========

.. glossary::
    :sorted:

    annotations
        Annotations are a concept used internally by SQLAlchemy in order to store
        additional information along with :class:`.ClauseElement` objects.  A Python
        dictionary is associated with a copy of the object, which contains key/value
        pairs significant to various internal systems, mostly within the ORM::

            some_column = Column('some_column', Integer)
            some_column_annotated = some_column._annotate({"entity": User})

        The annotation system differs from the public dictionary :attr:`.Column.info`
        in that the above annotation operation creates a *copy* of the new :class:`.Column`,
        rather than considering all annotation values to be part of a single
        unit.  The ORM creates copies of expression objects in order to
        apply annotations that are specific to their context, such as to differentiate
        columns that should render themselves as relative to a joined-inheritance
        entity versus those which should render relative to their immediate parent
        table alone, as well as to differentiate columns within the "join condition"
        of a relationship where the column in some cases needs to be expressed
        in terms of one particular table alias or another, based on its position
        within the join expression.
        Edited.

