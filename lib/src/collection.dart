part of 'cassandart_impl.dart';

abstract class _ListView<T> extends ListBase<T>
    implements UnmodifiableListView<T> {
  @override
  set length(int length) {
    _throw();
  }

  @override
  void operator []=(int index, T value) {
    _throw();
  }

  @override
  void clear() {
    _throw();
  }

  @override
  bool remove(Object element) {
    _throw();
    return null;
  }

  @override
  void removeWhere(bool test(T element)) {
    _throw();
  }

  @override
  void retainWhere(bool test(T element)) {
    _throw();
  }
}

void _throw() {
  throw new UnsupportedError('Cannot modify an unmodifiable List');
}

class CombinedListView<T> extends _ListView<T> {
  final List<List<T>> _lists;
  CombinedListView(this._lists);

  @override
  int get length => _lists.fold(0, (length, list) => length + list.length);

  @override
  T operator [](int index) {
    var initialIndex = index;
    for (var i = 0; i < _lists.length; i++) {
      var list = _lists[i];
      if (index < list.length) {
        return list[index];
      }
      index -= list.length;
    }
    throw new RangeError.index(initialIndex, this, 'index', null, length);
  }
}

class OffsetListView<T> extends _ListView<T> {
  final List<T> _list;
  final int _offset;
  final int _length;

  OffsetListView(this._list, this._offset) : _length =  _list.length - _offset;

  int get length => _length;

  T operator [](int index) {
    if (index < 0 || index > _length) {
      throw new RangeError.index(index, this, 'index', null, length);
    }
    return _list[index + _offset];
  }
}

class LimitListView<T> extends _ListView<T> {
  final List<T> _list;
  final int _length;

  LimitListView(this._list, this._length);

  int get length => _length;

  T operator [](int index) {
    if (index < 0 || index > _length) {
      throw new RangeError.index(index, this, 'index', null, length);
    }
    return _list[index];
  }
}
