/* _ring_native — the minimal C accelerator for the Python Ring connector.
 *
 * Python's stdlib has no cross-process atomics, which a shared-memory ring
 * fundamentally needs. This extension provides exactly that — atomic
 * load/store/CAS/swap/fetch-add on a word inside a writable buffer (the mmap),
 * plus the Linux futex syscall. Everything else (rings, frames, Avro, the
 * control protocol) is implemented in pure Python.
 *
 * Each function takes a writable buffer (the mmap) and a byte offset.
 */
#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <linux/futex.h>
#include <stdint.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

static int get_ptr(PyObject *args, void **base, Py_ssize_t *off, Py_buffer *view) {
    if (!PyArg_ParseTuple(args, "w*n", view, off)) return -1;
    if (*off < 0 || (size_t)*off + 8 > (size_t)view->len) {
        PyBuffer_Release(view);
        PyErr_SetString(PyExc_ValueError, "offset out of range");
        return -1;
    }
    *base = (char *)view->buf + *off;
    return 0;
}

static PyObject *load_u32(PyObject *self, PyObject *args) {
    (void)self;
    Py_buffer v;
    Py_ssize_t off;
    void *p;
    if (get_ptr(args, &p, &off, &v)) return NULL;
    uint32_t r = __atomic_load_n((uint32_t *)p, __ATOMIC_ACQUIRE);
    PyBuffer_Release(&v);
    return PyLong_FromUnsignedLong(r);
}

static PyObject *store_u32(PyObject *self, PyObject *args) {
    (void)self;
    Py_buffer v;
    Py_ssize_t off;
    unsigned long val;
    /* parse manually: buffer, offset, value */
    if (!PyArg_ParseTuple(args, "w*nk", &v, &off, &val)) return NULL;
    if (off < 0 || (size_t)off + 4 > (size_t)v.len) {
        PyBuffer_Release(&v);
        PyErr_SetString(PyExc_ValueError, "offset out of range");
        return NULL;
    }
    __atomic_store_n((uint32_t *)((char *)v.buf + off), (uint32_t)val, __ATOMIC_RELEASE);
    PyBuffer_Release(&v);
    Py_RETURN_NONE;
}

static PyObject *load_u64(PyObject *self, PyObject *args) {
    (void)self;
    Py_buffer v;
    Py_ssize_t off;
    void *p;
    if (get_ptr(args, &p, &off, &v)) return NULL;
    uint64_t r = __atomic_load_n((uint64_t *)p, __ATOMIC_ACQUIRE);
    PyBuffer_Release(&v);
    return PyLong_FromUnsignedLongLong(r);
}

static PyObject *store_u64(PyObject *self, PyObject *args) {
    (void)self;
    Py_buffer v;
    Py_ssize_t off;
    unsigned long long val;
    if (!PyArg_ParseTuple(args, "w*nK", &v, &off, &val)) return NULL;
    if (off < 0 || (size_t)off + 8 > (size_t)v.len) {
        PyBuffer_Release(&v);
        PyErr_SetString(PyExc_ValueError, "offset out of range");
        return NULL;
    }
    __atomic_store_n((uint64_t *)((char *)v.buf + off), (uint64_t)val, __ATOMIC_RELEASE);
    PyBuffer_Release(&v);
    Py_RETURN_NONE;
}

static PyObject *cas_u32(PyObject *self, PyObject *args) {
    (void)self;
    Py_buffer v;
    Py_ssize_t off;
    unsigned long expected, desired;
    if (!PyArg_ParseTuple(args, "w*nkk", &v, &off, &expected, &desired)) return NULL;
    if (off < 0 || (size_t)off + 4 > (size_t)v.len) {
        PyBuffer_Release(&v);
        PyErr_SetString(PyExc_ValueError, "offset out of range");
        return NULL;
    }
    uint32_t exp = (uint32_t)expected;
    int ok = __atomic_compare_exchange_n((uint32_t *)((char *)v.buf + off), &exp,
                                         (uint32_t)desired, 0, __ATOMIC_ACQUIRE,
                                         __ATOMIC_RELAXED);
    PyBuffer_Release(&v);
    return PyBool_FromLong(ok);
}

static PyObject *swap_u32(PyObject *self, PyObject *args) {
    (void)self;
    Py_buffer v;
    Py_ssize_t off;
    unsigned long val;
    if (!PyArg_ParseTuple(args, "w*nk", &v, &off, &val)) return NULL;
    if (off < 0 || (size_t)off + 4 > (size_t)v.len) {
        PyBuffer_Release(&v);
        PyErr_SetString(PyExc_ValueError, "offset out of range");
        return NULL;
    }
    uint32_t old = __atomic_exchange_n((uint32_t *)((char *)v.buf + off), (uint32_t)val,
                                       __ATOMIC_ACQUIRE);
    PyBuffer_Release(&v);
    return PyLong_FromUnsignedLong(old);
}

static PyObject *fetch_add_u32(PyObject *self, PyObject *args) {
    (void)self;
    Py_buffer v;
    Py_ssize_t off;
    long delta;
    if (!PyArg_ParseTuple(args, "w*nl", &v, &off, &delta)) return NULL;
    if (off < 0 || (size_t)off + 4 > (size_t)v.len) {
        PyBuffer_Release(&v);
        PyErr_SetString(PyExc_ValueError, "offset out of range");
        return NULL;
    }
    uint32_t old = __atomic_fetch_add((uint32_t *)((char *)v.buf + off), (uint32_t)delta,
                                      __ATOMIC_RELEASE);
    PyBuffer_Release(&v);
    return PyLong_FromUnsignedLong(old);
}

static PyObject *futex_wait(PyObject *self, PyObject *args) {
    (void)self;
    Py_buffer v;
    Py_ssize_t off;
    unsigned long expected;
    long timeout_ms;
    if (!PyArg_ParseTuple(args, "w*nkl", &v, &off, &expected, &timeout_ms)) return NULL;
    if (off < 0 || (size_t)off + 4 > (size_t)v.len) {
        PyBuffer_Release(&v);
        PyErr_SetString(PyExc_ValueError, "offset out of range");
        return NULL;
    }
    uint32_t *addr = (uint32_t *)((char *)v.buf + off);
    struct timespec ts, *tp = NULL;
    if (timeout_ms >= 0) {
        ts.tv_sec = timeout_ms / 1000;
        ts.tv_nsec = (long)(timeout_ms % 1000) * 1000000L;
        tp = &ts;
    }
    Py_BEGIN_ALLOW_THREADS
    syscall(SYS_futex, addr, FUTEX_WAIT, (uint32_t)expected, tp, NULL, 0);
    Py_END_ALLOW_THREADS
    PyBuffer_Release(&v);
    Py_RETURN_NONE;
}

static PyObject *futex_wake(PyObject *self, PyObject *args) {
    (void)self;
    Py_buffer v;
    Py_ssize_t off;
    int n;
    if (!PyArg_ParseTuple(args, "w*ni", &v, &off, &n)) return NULL;
    if (off < 0 || (size_t)off + 4 > (size_t)v.len) {
        PyBuffer_Release(&v);
        PyErr_SetString(PyExc_ValueError, "offset out of range");
        return NULL;
    }
    uint32_t *addr = (uint32_t *)((char *)v.buf + off);
    long r = syscall(SYS_futex, addr, FUTEX_WAKE, n, NULL, NULL, 0);
    PyBuffer_Release(&v);
    return PyLong_FromLong(r);
}

static PyMethodDef methods[] = {
    {"load_u32", load_u32, METH_VARARGS, "atomic acquire load u32"},
    {"store_u32", store_u32, METH_VARARGS, "atomic release store u32"},
    {"load_u64", load_u64, METH_VARARGS, "atomic acquire load u64"},
    {"store_u64", store_u64, METH_VARARGS, "atomic release store u64"},
    {"cas_u32", cas_u32, METH_VARARGS, "atomic compare-exchange u32"},
    {"swap_u32", swap_u32, METH_VARARGS, "atomic exchange u32"},
    {"fetch_add_u32", fetch_add_u32, METH_VARARGS, "atomic fetch-add u32"},
    {"futex_wait", futex_wait, METH_VARARGS, "FUTEX_WAIT on a word"},
    {"futex_wake", futex_wake, METH_VARARGS, "FUTEX_WAKE on a word"},
    {NULL, NULL, 0, NULL},
};

static struct PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT, "_ring_native",
    "Cross-process atomics + futex for the Ring Python connector.", -1, methods,
};

PyMODINIT_FUNC PyInit__ring_native(void) { return PyModule_Create(&moduledef); }
