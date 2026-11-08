-- A small table to build datasets, charts and alerts on.
CREATE TABLE public.tf_review_sales (
    id integer PRIMARY KEY,
    amount numeric(10, 2),
    day date
);

INSERT INTO public.tf_review_sales (id, amount, day) VALUES
    (1, 10.50, '2026-01-01'),
    (2, 20.00, '2026-01-02');
