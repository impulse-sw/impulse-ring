from setuptools import Extension, setup

setup(
    name="impulse-ring",
    version="0.1.0",
    description="Native Python connector for Ring (Impulse) — shared-memory IPC",
    packages=["impulse_ring"],
    ext_modules=[
        Extension(
            "impulse_ring._ring_native",
            sources=["src/_ring_native.c"],
            extra_compile_args=["-O2", "-Wall"],
        )
    ],
    python_requires=">=3.8",
)
