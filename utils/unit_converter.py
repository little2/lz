class UnitConverter:
    def __init__(self, use_si=False):
        self.use_si = use_si

    def byte_to_human_readable(self, num_bytes):
        units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
        step = 1000.0 if self.use_si else 1024.0

        if num_bytes < 0:
            raise ValueError("字节数不能为负数")

        index = 0
        num = float(num_bytes)

        while num >= step and index < len(units) - 1:
            num /= step
            index += 1

        return f"{num:.2f} {units[index]}"

    def seconds_to_hms(self, seconds):
        if seconds < 0:
            raise ValueError("秒数不能为负数")

        h = int(seconds // 3600)
        m = int((seconds % 3600) // 60)
        s = int(seconds % 60)

        return f"{h:02d}:{m:02d}:{s:02d}"

    def meters_to_kilometers(self, meters):
        if meters < 0:
            raise ValueError("米不能为负数")

        km = meters / 1000.0
        return f"{km:.2f} km"
