"""Home-channel onboarding notice routing rules."""

from types import SimpleNamespace

from gateway.config import Platform
from gateway.run import _should_send_home_channel_notice


def _source(platform):
    return SimpleNamespace(platform=platform)


def test_home_channel_notice_is_suppressed_for_queue_control_plane():
    """Queue handoffs route by target, not by a user-facing home channel."""
    assert not _should_send_home_channel_notice(
        _source(SimpleNamespace(value="queue")), history=[]
    )


def test_home_channel_notice_is_suppressed_for_local_and_webhook():
    assert not _should_send_home_channel_notice(_source(Platform.LOCAL), history=[])
    assert not _should_send_home_channel_notice(_source(Platform.WEBHOOK), history=[])


def test_home_channel_notice_still_fires_for_user_facing_first_contact():
    assert _should_send_home_channel_notice(_source(Platform.SLACK), history=[])


def test_home_channel_notice_does_not_repeat_when_history_exists():
    assert not _should_send_home_channel_notice(
        _source(Platform.SLACK), history=[{"role": "user", "content": "hi"}]
    )


def test_home_channel_notice_requires_platform():
    assert not _should_send_home_channel_notice(_source(None), history=[])
