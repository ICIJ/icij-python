import datetime

from batch_download_utils.migrate_task_manager_batch_downloads import rename_field, rename_value, add_field, \
    get_date_from_task, move_field


def test_rename_basic_field():
    assert rename_field({"foo": "bar"}, "foo", "baz") == {"baz": "bar"}


def test_rename_nested_field():
    assert rename_field({"foo": {"bar": "baz"}}, "bar", "qux") == {"foo": {"qux": "baz"}}


def test_rename_value():
    assert rename_value({"foo": "bar"}, "bar", "qux") == {"foo": "qux"}


def test_add_field():
    assert add_field({"foo": "bar"}, "baz", "qux") == {"foo": "bar", "baz": "qux"}


def test_move_field():
    assert move_field({"foo": "bar"}, "foo", "level1.foo") == {"level1": {"foo": "bar"}}
    assert move_field({"foo": "bar"}, "foo", "level1.level2.foo") == {"level1": {"level2": {"foo": "bar"}}}


def test_move_field_without_nesting():
    assert move_field({"foo": "bar"}, "foo", "baz") == {"baz":  "bar"}


def test_get_date_from_task_for_batch_download():
    assert get_date_from_task({"args": {"batchDownload": {"filename": ["java.nio.file.Path",  "file:///home/dev/.local/share/datashare/tmp/archive_local_2024-08-20T07_27_42.192Z%5BGMT%5D.zip"]}}}) == datetime.datetime.fromisoformat("2024-08-20T07:27:42.192")


def test_get_date_from_task_for_batch_search():
    date = get_date_from_task({"@type":"Task","id":"771c45b4-1fac-421f-9791-64ac4e1eb4ab","name":"org.icij.datashare.tasks.BatchSearchRunner","state":"DONE","progress":1.0,"user":{"@type":"org.icij.datashare.session.DatashareUser","id":"fbar","name":"Foo Bar","email":"fbar@icij.org","provider":"icij"},"result":5,"args":{"@type":"java.util.Collections$UnmodifiableMap"}})
    assert datetime.datetime.now() - date < datetime.timedelta(seconds=1)


def test_get_date_from_task_for_tasks():
    a_date = datetime.datetime.fromisoformat("2024-08-20T07:27:42.192")
    date = get_date_from_task({"@type":"Task","id":"771c45b4-1fac-421f-9791-64ac4e1eb4ab","name":"my_task", "createdAt": a_date.timestamp()})
    assert date == a_date
