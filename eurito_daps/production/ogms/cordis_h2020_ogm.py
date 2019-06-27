from py2neo.ogm import GraphObject, Property, RelatedFrom, RelatedTo, RelatedObjects


class Projects(GraphObject):
    __primarykey__ = 'rcn'

    rcn = Property()
    id = Property()
    acronym = Property()
    status = Property()
    # programme = Property()
    # framework_programme = Property()
    topics = Property()
    title = Property()
    start_date = Property()
    end_date = Property()
    project_url = Property()
    objective = Property()
    total_cost = Property()
    ec_max_contribution = Property()
    call = Property()
    funding_scheme = Property()
    # coordinator = Property()
    # coordinator_country = Property()
    # participants = Property()
    # participant_countries = Property()

    delivers = RelatedTo('ProjectDeliverables', 'DELIVERS')
    produces = RelatedTo('Reports', 'PRODUCES')
    part_of = RelatedTo('Programmes', 'PART_OF')
    publishes = RelatedTo('Reports', 'PUBLISHES')
    participated_by = RelatedFrom('Organizations')
    # part_of = RelatedObjects('Projects', 1, 'PARTICIPATED_BY', 'Organizations')
    # coordinated_by = RelatedTo('Organizations', 'COORDINATED_BY')
    # participated_by = RelatedTo('Organizations', 'PARTICIPATED_BY')

# class PartOF(GraphObject):
#     role = Property()


class Organizations(GraphObject):
    __primarykey__ = 'id'

    project_rcn = Property()
    role = Property()
    id = Property()
    name = Property()
    short_name = Property()
    activity_type = Property()
    end_of_participation = Property()
    ec_contribution = Property()
    # country = Property()
    street = Property()
    city = Property()
    post_code = Property()
    organization_url = Property()
    vat_number = Property()
    contact_form = Property()
    contact_type = Property()
    contact_title = Property()
    contact_first_names = Property()
    contact_last_names = Property()
    contact_telephone_number = Property()
    contact_fax_number = Property()

    participates_in = RelatedTo('Organizations', 'PARTICIPATES_IN')

    # coordinated = RelatedFrom(Projects, 'COORDINATED_BY')
    # participated_in = RelatedFrom(Projects, 'PARTICIPATED_BY')


class ProjectPublications(GraphObject):
    __primarykey__ = 'title'

    rcn = Property()
    title = Property()
    # project_id = Property()
    # project_acronym = Property()
    # programme = Property()
    # topics = Property()
    authors = Property()
    journal_title = Property()
    journal_number = Property()
    published_year = Property()
    published_pages = Property()
    issn = Property()
    doi = Property()
    is_published_as = Property()
    last_update_date = Property()


class Programmes(GraphObject):
    __primarykey__ = 'id'

    id = Property()
    framework_programme = Property()

    has_project = RelatedFrom(Projects)
    part_of = RelatedTo('FrameworkProgramme', 'PART_OF')


class FrameworkProgramme(GraphObject):
    __primarykey__ = 'id'

    id = Property()
    name = Property()

    has_framework = RelatedFrom(Programmes)


class Reports(GraphObject):
    __primarykey__ = 'title'

    rcn = Property()
    # language = Property()
    title = Property()
    teaser = Property()
    summary = Property()
    work_performed = Property()
    final_results = Property()
    last_update_date = Property()
    # project_id = Property()
    # project_acronym = Property()
    # programme = Property()
    # topics = Property()
    related_file = Property()
    url = Property()

    published_by = RelatedFrom(Projects)


class ProjectDeliverables(GraphObject):
    __primarykey__ = 'title'

    rcn = Property()
    title = Property()
    description = Property()
    deliverable_type = Property()
    url = Property()
    last_update_date = Property()

    delivered_from = RelatedFrom(Projects)
