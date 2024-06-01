
from typing import Iterable, Iterator, TypeVar
from threading import Thread
from collections import deque
from time import sleep
import logging
import json
import math
from pathlib import Path
from itertools import pairwise
from argparse import ArgumentParser
from PIL import Image
# from kafka import KafkaConsumer, KafkaProducer
from astar import AStar
from szpak import KafkaConsumer, KafkaProducer

T = TypeVar('T')

Coord = tuple[int, int]
Way = tuple[Coord, ...]

SR2 = math.sqrt(2)


def around_border_iter(seq: Iterable[T], default: T | None = None) -> Iterator[tuple[T | None, T, T | None]]:
    """a,b,c -> (None, a, b), (a, b, c), (b, c, None)."""
    it = iter(seq)
    prv = default
    val = next(it)
    while True:
        try:
            nxt = next(it)
        except StopIteration:
            yield prv, val, default
            break
        yield prv, val, nxt


def around_iter(seq: Iterable[T]) -> Iterator[tuple[T, T, T]]:
    """a,b,c,d,e -> (a, b, c), (b, c, d), (c, d, e)."""
    # pairwise('ABCDEFG') → AB BC CD DE EF FG
    it = iter(seq)
    a = next(it, None)
    b = next(it, None)
    for c in it:
        yield a, b, c  # type: ignore  - `a` and `b` is never None
        a, b = b, c


def consumer():
    kafka = KafkaConsumer('aaa',
                          bootstrap_servers='localhost:9092',
                          value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                          api_version=(3, 7, 0),
                          )
    print('C')
    for msg in kafka:
        print(f'recv: {msg = }')
        break


def producer():
    kafka = KafkaProducer(bootstrap_servers='localhost:9092',
                          value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                          api_version=(3, 7, 0),
                          )
    print('P')
    kafka.send('aaa', 'zażółć')


class MapPath(AStar):

    def __init__(self, map: 'Map') -> None:
        # wall = Map.WALL
        self.points = {(x, y): c
                       for y, row in enumerate(map.map)
                       for x, c in enumerate(row)
                       if (x, y) in map.places or c in Map.WALKABLE}
                       # if c not in wall}

    def heuristic_cost_estimate(self, current: Coord, goal: Coord) -> float:
        """computes the 'direct' distance between two (x,y) tuples."""
        x1, y1 = current
        x2, y2 = goal
        return math.hypot(x2 - x1, y2 - y1)

    def distance_between(self, n1: Coord, n2: Coord):
        """this method always returns 1, as two 'neighbors' are always adajcent."""
        x1, y1 = n1
        x2, y2 = n2
        return 1 if x1 == x2 or y1 == y2 else SR2

    def neighbors(self, node) -> Iterable[Coord]:
        """
        For a given coordinate in the maze, returns up to 4 adjacent(north,east,south,west)
        nodes that can be reached (=any adjacent coordinate that is not a wall).
        """
        x, y = node
        return [p for dx in range(-1, 2) for dy in range(-1, 2)
                if (p := (x + dx, y + dy)) != node and self.points.get(p)]

class Map:
    LEGEND = {
        'X': (0, 0, 0),
        ' ': (255, 255, 255),
        'Z': (0x00, 0x77, 0x04),
        'B': (0xad, 0x08, 0x08),
        'L': (0x3a, 0x3d, 0xf8),
    }
    WALL = 'Xx'
    WALKABLE = ' '

    def __init__(self):
        self.map: list[str] = []
        self._width: int = 0
        self._height: int = 0
        self.places: dict[Coord, str] = {}  # wejścia
        self.borders: dict[Coord, str] = {}

    @property
    def width(self) -> int:
        return self._width

    @property
    def height(self) -> int:
        return self._height

    def load(self, path: str | Path) -> None:
        pixels = {v: k for k, v in self.LEGEND.items()}
        with Image.open(path) as im:
            self._width, self._height = im.width, im.height
            self.map = [''.join(pixels.get(im.getpixel((x, y)), 'x')
                                for x in range(im.width))
                        for y in range(im.height)]
        # Szukanie sklepów (bez granicznych linii mapy).
        self.borders = {}
        for y, (prow, row, nrow) in enumerate(around_iter(self.map), 1):
            for x, (w, c, e) in enumerate(around_iter(row), 1):
                n, s = prow[x], nrow[x]
                if c not in self.WALL and c not in self.WALKABLE and any(p in self.WALKABLE for p in (n, s, w, e)):
                    self.borders[x, y] = c
        borders = dict(self.borders)  # kopia
        # Szuaknie wejść do sklepów.
        while borders:
            p, c = next(iter(borders.items()))
            stack = [p]
            self.places[p] = c
            while stack:
                p = stack.pop()
                x, y = p
                # del borders[p]
                if borders.pop(p, None):
                    for dy in range(-1, 2):
                        for dx in range(-1, 2):
                            pp = x + dx, y + dy
                            if pp != p and borders.get(pp) == c:
                                stack.append(pp)

    # def find_path(self, p1: tuple[int, int], p2: tuple[int, int]):
    #     # dist = [[10**99] * self.width] * self._height
    #     dmap: dict[tuple[int, int], int] = {p1: 0}
    #     nxt = deque()
    #     x, y = p1
    #     nxt.append((x, y))
    #     walk = ' '
    #     MAX = 10**99
    #     while nxt:
    #         x, y = nxt.popleft()
    #         dist = min(dmap.get(p, MAX)
    #                    for p in ((x-1, y), (x+1, y), (x, y-1), (x,y+1)))
    #         dist = min(dmap.get((x, y), MAX), dist + 1)
    #         if x > 0 and self.map[y][x-1] in walk:
    #             dmap[x-1, y] = dist
    #             nxt.append((x-1, y))
    #

    def find_path(self, p1: Coord, p2: Coord) -> Way | None:
        way = MapPath(self).astar(p1, p2)
        if way is None:
            return None
        return tuple(way)

    def find_near_places(self, current: Coord, radius: int) -> tuple[Way, ...]:
        def get():
            x1, y1 = current
            for place in self.places:
                x2, y2 = place
                if math.hypot(x2 - x1, y2 - y1) <= radius:
                    print(f'{place = }')
                    if way := self.find_path(current, place):
                        yield way

        return tuple(get())


def run(map_path: Path) -> None:
    # logging.basicConfig(level=logging.DEBUG)
    my_map = Map()
    my_map.load(map_path)
    ziutek = (233, 444)
    # way = my_map.find_path(ziutek, (380, 280))
    # way = my_map.find_path(ziutek, (900, 890))
    # way = my_map.find_path(ziutek, (300, 450))
    # print(way)
    ways = my_map.find_near_places(ziutek, 120)
    with Image.open(map_path) as im:
        im2 = im.copy()
        # if way:
        #     for p in way:
        #         im2.putpixel(p, (255, 128, 128))
        for way in ways:
            for p in way:
                im2.putpixel(p, (255, 128, 128))
        for p in my_map.borders:
            im2.putpixel(p, (255, 255, 64))
        for p in my_map.places:
            im2.putpixel(p, (64, 255, 255))
        im2.save(map_path.with_stem(f'{map_path.stem}a'))
    return
    c = Thread(target=consumer)
    p = Thread(target=producer)
    c.start()
    p.start()
    c.join()
    p.join()


def main(argv: list[str] | None = None) -> None:
    p = ArgumentParser()
    p.add_argument('map', type=Path, help='path to PNG map')
    args = p.parse_args(argv)
    run(map_path=args.map)


    #test githuba
    # dodanie na remote repository


if __name__ == '__main__':
    main()
