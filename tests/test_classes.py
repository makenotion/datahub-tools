from datahub_tools.classes import DHTag


def test_dhtag():
    priority = 'P0'
    priority_tag = DHTag(name=f'Priority: {priority}', urn=f'urn:li:tag:Priority: {priority}')
    assert priority_tag.is_priority()
    assert priority_tag.get_priority() == priority
