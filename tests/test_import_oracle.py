from scripts import import_oracle

platforms = {
    "linux": import_oracle.ev.LINUX_TNS,
    "win32": import_oracle.ev.WINDOWS_TNS,
}


def test_platforms(monkeypatch):
    for key, value in platforms.items():
        print(key, value)
        monkeypatch.setattr(import_oracle, "platform", f"{key}")
        assert import_oracle.test_platform() == platforms[key]
