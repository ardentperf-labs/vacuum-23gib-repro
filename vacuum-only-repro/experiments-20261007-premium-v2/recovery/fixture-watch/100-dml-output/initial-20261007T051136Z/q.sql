CREATE INDEX vacuum_target_stage_id_idx ON public.vacuum_target_stage(id); DELETE FROM public.vacuum_target_stage WHERE id%2=1; CHECKPOINT;
