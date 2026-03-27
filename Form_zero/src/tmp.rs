
        } /*1565*/ /*safe*/

        let filtered = batch /*1567*/ /*safe*/
            batch.rerouted_to_process_id, /*1601*/ /*safe*/
        ); /*1602*/ /*safe*/

        match self
            .append_mailbox_batch_to_live(&target, &content, patch)
            .await
        {
        let _ = self /*1604*/
            .sequence_engine /*1605*/
            .ingest(RuntimeSignal { /*1606*/
                kind: RuntimeSignalKind::ReceiveStarted, /*1607*/
                observed_process_id: target.process.id, /*1608*/
                observed_stream_id: None, /*1609*/
                observed_slot: target /*1610*/
                    .binding /*1611*/
                    .as_ref() /*1612*/
                    .map(|binding| binding.program_slot_name.clone()), /*1613*/
                executor_process_id: None, /*1614*/
                segment_kind: Some(MAILBOX_MESSAGE_SEGMENT_KIND.to_string()), /*1615*/
                content: Some(content.clone()), /*1616*/
                tool_name: None, /*1617*/
                tool_run_id: None, /*1618*/
                metadata: patch.clone(), /*1619*/
                observed_at: Utc::now(), /*1620*/
            }) /*1621*/
            .await; /*1622*/

        match self.append_mailbox_batch_to_live(&target, &content, patch).await { /*1624*/
            Ok(segment) => { /*1625*/
                let _ = self /*1626*/
                    .sequence_engine /*1627*/
                    }) /*1643*/
                    .await; /*1644*/

                let actions = self /*1646*/ /*safe*/
                    .sequence_engine /*1647*/ /*safe*/
                    .ingest(RuntimeSignal { /*1648*/ /*safe*/
                        kind: RuntimeSignalKind::ReceiveCompleted, /*1649*/
                        observed_process_id: target.process.id, /*1650*/ /*safe*/
                        observed_stream_id: None, /*1651*/ /*safe*/
                        observed_slot: target /*1652*/ /*safe*/
                            .binding /*1653*/ /*safe*/
                            .as_ref() /*1654*/ /*safe*/
                            .map(|binding| binding.program_slot_name.clone()), /*1655*/ /*safe*/
                        executor_process_id: None, /*1656*/ /*safe*/
                        segment_kind: Some(segment.segment_kind.clone()), /*1657*/ /*safe*/
                        content: Some(segment.content.clone()), /*1658*/ /*safe*/
                        tool_name: None, /*1659*/ /*safe*/
                        tool_run_id: None, /*1660*/ /*safe*/
                        metadata: segment.patch.clone(), /*1661*/ /*safe*/
                        observed_at: Utc::now(), /*1662*/ /*safe*/
                    }) /*1663*/ /*safe*/
                    .await; /*1664*/ /*safe*/
                let _ = self.apply_sequence_actions(actions).await; /*1665*/ /*safe*/

                let _ = self /*1667*/ /*safe*/
                    .record_forced_event_results(batch.info.batch_id, &filtered, &target) /*1668*/ /*safe*/
                    .await; /*1669*/ /*safe*/
            &self.sequence_engine, /*1705*/ /*safe*/
            &target.process, /*1706*/ /*safe*/
            target.binding.as_ref(), /*1707*/ /*safe*/
            "mailbox_message",
            MAILBOX_MESSAGE_SEGMENT_KIND, /*1708*/
            content, /*1709*/ /*safe*/
            Some("whitespace"), /*1710*/ /*safe*/
            patch, /*1711*/ /*safe*/
        .await /*1716*/ /*safe*/
    } /*1717*/ /*safe*/

    async fn force_boundary_for_process(&self, target: &ProcessRuntimeBinding) -> Result<()> {
    async fn force_interrupt_for_process(&self, target: &ProcessRuntimeBinding) -> Result<()> { /*1719*/
        let Some(open_live) = self.live_bus_registry.get(target.process.id).await else { /*1720*/ /*safe*/
            return Ok(()); /*1721*/ /*safe*/
        }; /*1722*/ /*safe*/
        let decision = self
        let actions = self /*1723*/
            .sequence_engine /*1724*/ /*safe*/
            .ingest(RuntimeSignal { /*1725*/ /*safe*/
                kind: RuntimeSignalKind::ForceBoundary,
                kind: RuntimeSignalKind::ForceInterrupted, /*1726*/
                observed_process_id: target.process.id, /*1727*/ /*safe*/
                observed_stream_id: None, /*1728*/ /*safe*/
                observed_slot: target /*1729*/ /*safe*/
                observed_at: Utc::now(), /*1739*/ /*safe*/
            }) /*1740*/ /*safe*/
            .await; /*1741*/ /*safe*/
        if matches!(decision, BoundaryDecision::SealNow) {
            seal_persisted_live_to_pending(
                &self.live_bus_registry,
                &self.runtime_head_registry,
                &self.sequence_engine, /*2109*/
                &target.process,
                target.binding.as_ref(),
                None,
                None,
                None,
            )
            .await?;
        }
        self.apply_sequence_actions(actions).await?; /*1742*/ /*safe*/
        Ok(()) /*1743*/ /*safe*/
    } /*1744*/ /*safe*/

            reply_to: None, /*2023*/ /*safe*/
            created_at: Utc::now(), /*2024*/ /*safe*/
        }; /*2025*/ /*safe*/
        self.global_message_queue.enqueue(bounced)
        let target_process_id = bounced.target_process_id; /*2026*/
        self.global_message_queue.enqueue(bounced)?; /*2027*/
        self.request_release_for_process(target_process_id, "bounce_message_send") /*2028*/
            .await; /*2029*/
        Ok(()) /*2030*/ /*safe*/
    } /*2031*/ /*safe*/

    async fn blacklist_sender_and_drop(&self, envelope: QueueEnvelope) -> Result<()> { /*2033*/ /*safe*/
                .db /*2102*/ /*safe*/
                .get_process_runtime_binding(envelope.sender_process_id) /*2103*/ /*safe*/
                .await?; /*2104*/ /*safe*/
            let _ = append_process_segment_to_live_bus(
            let _ = append_process_body_to_live_bus( /*2105*/
                &self.db, /*2106*/ /*safe*/
                &self.live_bus_registry, /*2107*/ /*safe*/
                &self.runtime_head_registry, /*2108*/ /*safe*/
        .unwrap_or_else(|| "monitor_trigger".to_string()) /*2332*/ /*safe*/
} /*2333*/ /*safe*/

fn render_send_message_trace(envelope: &QueueEnvelope) -> String {
fn render_send_message_trace_started(envelope: &QueueEnvelope) -> String { /*2335*/
    let mut content = format!( /*2336*/
        "[send_message]\nstatus: started\nmessage_id: {}\nmessage_kind: {}\ntarget_process_id: {}\nbase_priority: {}\nrequested_delivery_action: {}\n", /*2337*/
        envelope.message_id, /*2338*/
        content.push('\n'); /*2360*/
    } /*2361*/
    content.push('\n'); /*2362*/
    content /*2363*/
} /*2364*/

fn render_send_message_trace_completion(envelope: &QueueEnvelope) -> String { /*2366*/
    let mut content = String::new(); /*2367*/
    content.push_str("status: completed\nfinal_priority: "); /*2368*/
    content.push_str( /*2369*/
        &envelope /*2370*/
        request: SendMessageRequest, /*2579*/ /*safe*/
    ) -> Result<QueueEnvelope> { /*2580*/ /*safe*/
        let sender_binding = self.resolve_target_process(&request.sender).await?; /*2581*/ /*safe*/
        let _process_stream_permit = /*2582*/ /*safe*/
            self.acquire_process_stream_slot(sender_binding.process.id).await?; /*2583*/ /*safe*/
        let target_binding = self.resolve_target_process(&request.target).await?; /*2584*/ /*safe*/
        let explicit_result_binding = match request.explicit_result_target.as_ref() { /*2585*/ /*safe*/
            Some(selector) => Some(self.resolve_target_process(selector).await?), /*2586*/ /*safe*/
                request.metadata, /*2604*/ /*safe*/
            ) /*2605*/ /*safe*/
            .await?; /*2606*/ /*safe*/
        self.handle_send_message_started(&sender_binding, &envelope) /*2607*/ /*safe*/
            .await?; /*2608*/ /*safe*/
        self.global_message_queue.enqueue(envelope.clone())?; /*2609*/ /*safe*/
        self.handle_send_message_completed(&sender_binding, &envelope)
        self.handle_send_message_completed(&sender_binding, &target_binding, &envelope) /*2610*/
            .await?; /*2611*/ /*safe*/
        Ok(envelope) /*2612*/ /*safe*/
    } /*2613*/ /*safe*/

    async fn handle_send_message_started( /*2615*/ /*safe*/
        &self, /*2616*/ /*safe*/
        sender: &ProcessRuntimeBinding, /*2617*/ /*safe*/
        envelope: &QueueEnvelope, /*2618*/ /*safe*/
    ) -> Result<()> { /*2619*/ /*safe*/
        let actions = self /*2620*/ /*safe*/
            .sequence_engine /*2621*/ /*safe*/
            .ingest(RuntimeSignal { /*2622*/ /*safe*/
                kind: RuntimeSignalKind::SendStarted, /*2623*/
                observed_process_id: sender.process.id, /*2624*/ /*safe*/
                observed_stream_id: None, /*2625*/ /*safe*/
                observed_slot: sender /*2626*/ /*safe*/
                    .binding /*2627*/ /*safe*/
                    .as_ref() /*2628*/ /*safe*/
                    .map(|binding| binding.program_slot_name.clone()), /*2629*/ /*safe*/
                executor_process_id: Some(sender.process.id), /*2630*/ /*safe*/
                segment_kind: None, /*2631*/ /*safe*/
                content: None, /*2632*/ /*safe*/
                tool_name: Some("send_message".to_string()), /*2633*/ /*safe*/
                tool_run_id: None, /*2634*/ /*safe*/
                metadata: None, /*2635*/ /*safe*/
                observed_at: Utc::now(), /*2636*/ /*safe*/
            }) /*2637*/ /*safe*/
            .await; /*2638*/ /*safe*/
        self.queue_dispatcher.apply_sequence_actions(actions).await?; /*2639*/ /*safe*/

        let trace_content = render_send_message_trace_started(envelope); /*2641*/
        let _ = append_process_segment_to_live_bus( /*2642*/
            &self.db, /*2643*/ /*safe*/
            &self.live_bus_registry, /*2644*/ /*safe*/
            &self.runtime_head_registry, /*2645*/ /*safe*/
            &self.sequence_engine, /*2646*/ /*safe*/
            &sender.process, /*2647*/ /*safe*/
            sender.binding.as_ref(), /*2648*/ /*safe*/
            SEND_MESSAGE_TRACE_SEGMENT_KIND, /*2649*/ /*safe*/
            &trace_content, /*2650*/ /*safe*/
            Some("whitespace"), /*2651*/ /*safe*/
            Some(json!({ /*2652*/ /*safe*/
                "message_id": envelope.message_id.to_string(), /*2653*/ /*safe*/
                "message_kind": envelope.message_kind.as_str(), /*2654*/ /*safe*/
                "requested_delivery_action": envelope.requested_delivery_action.as_str(), /*2655*/ /*safe*/
            })), /*2656*/ /*safe*/
            Some(sender.process.id), /*2657*/ /*safe*/
            Some("send_message"), /*2658*/ /*safe*/
            None, /*2659*/ /*safe*/
        ) /*2660*/ /*safe*/
        .await?; /*2661*/ /*safe*/
        Ok(()) /*2662*/ /*safe*/
    } /*2663*/ /*safe*/

    async fn handle_send_message_completed( /*2665*/ /*safe*/
        &self, /*2666*/ /*safe*/
        sender: &ProcessRuntimeBinding, /*2667*/ /*safe*/
        target: &ProcessRuntimeBinding, /*2668*/ /*safe*/
        envelope: &QueueEnvelope, /*2669*/ /*safe*/
    ) -> Result<()> { /*2670*/ /*safe*/
        let trace_content = render_send_message_trace(envelope);
        let trace_content = render_send_message_trace_completion(envelope); /*2671*/
        let trace_segment = append_process_segment_to_live_bus( /*2672*/ /*safe*/
            &self.db, /*2673*/ /*safe*/
            &self.live_bus_registry, /*2674*/ /*safe*/
            &self.sequence_engine, /*2676*/ /*safe*/
            &sender.process, /*2677*/ /*safe*/
            sender.binding.as_ref(), /*2678*/ /*safe*/
            "send_message_trace",
            SEND_MESSAGE_TRACE_SEGMENT_KIND, /*2679*/
            &trace_content, /*2680*/ /*safe*/
            Some("whitespace"), /*2681*/ /*safe*/
            Some(json!({ /*2682*/
                    .final_delivery_action /*2687*/ /*safe*/
                    .map(|action| action.as_str().to_string()), /*2688*/ /*safe*/
                "final_priority": envelope.final_priority, /*2689*/ /*safe*/
                "release_target_process_id": target.process.id.to_string(), /*2690*/
            })), /*2691*/ /*safe*/
            Some(sender.process.id), /*2692*/ /*safe*/
            Some("send_message"), /*2693*/ /*safe*/
        .await?; /*2696*/ /*safe*/

        let signal = RuntimeSignal { /*2698*/ /*safe*/
            kind: RuntimeSignalKind::SendMessageCompleted,
            kind: RuntimeSignalKind::SendCompleted, /*2699*/
            observed_process_id: sender.process.id, /*2700*/ /*safe*/
            observed_stream_id: None, /*2701*/ /*safe*/
            observed_slot: sender /*2702*/ /*safe*/
            content: Some(trace_content), /*2708*/ /*safe*/
            tool_name: Some("send_message".to_string()), /*2709*/ /*safe*/
            tool_run_id: None, /*2710*/ /*safe*/
            metadata: trace_segment.patch.clone(),
            metadata: merge_patch( /*2711*/
                trace_segment.patch.clone(), /*2712*/
                Some(json!({ /*2713*/
                    "release_target_process_id": target.process.id.to_string(), /*2714*/
                })), /*2715*/
            ), /*2716*/
            observed_at: Utc::now(), /*2717*/ /*safe*/
        }; /*2718*/ /*safe*/
        let decision = self.sequence_engine.ingest(signal).await;
        let actions = self.sequence_engine.ingest(signal).await; /*2719*/

        self.enqueue_slot_monitor_triggers(sender).await?; /*2721*/ /*safe*/

        if matches!(decision, BoundaryDecision::SealNow) {
            let _ = seal_persisted_live_to_pending(
                &self.live_bus_registry,
                &self.runtime_head_registry,
                &self.sequence_engine, /*2109*/
                &sender.process,
                sender.binding.as_ref(),
                Some(sender.process.id),
                Some("send_message"),
                None,
            )
            .await?;
        }

        self.queue_dispatcher.release_for_process(sender.process.id).await;
        self.queue_dispatcher.apply_sequence_actions(actions).await?; /*2722*/ /*safe*/
        Ok(()) /*2723*/ /*safe*/
    } /*2724*/ /*safe*/

                ) /*2778*/ /*safe*/
                .await?; /*2779*/ /*safe*/
            self.global_message_queue.enqueue(envelope)?; /*2780*/ /*safe*/
            self.queue_dispatcher /*2781*/
                .request_release_for_process(target.process.id, "monitor_trigger_send") /*2782*/
                .await; /*2783*/
        } /*2784*/ /*safe*/

        Ok(()) /*2786*/ /*safe*/
                                process_id, /*2942*/ /*safe*/
                            ) /*2943*/ /*safe*/
                            .await?, /*2944*/ /*safe*/
                            PROVIDER_STREAM_SEGMENT_KIND.to_string(),
                            PROCESS_BODY_SEGMENT_KIND.to_string(), /*2945*/
                            Some(PROVIDER_STREAM_TOKENIZER.to_string()), /*2946*/ /*safe*/
                            provider_stream_patch(tool_name, tool_run_id, process_id, None), /*2959*/ /*safe*/
                            process_body_patch( /*2947*/
                                "token_stream", /*2948*/
                                provider_stream_patch(tool_name, tool_run_id, process_id, None), /*2949*/
                            ), /*2950*/
                        ), /*2951*/ /*safe*/
                    }; /*2952*/ /*safe*/
                    open_live.apply_delta( /*2953*/ /*safe*/
                        content_delta, /*2954*/ /*safe*/
                        Some(PROVIDER_STREAM_SEGMENT_KIND.to_string()),
                        Some(PROCESS_BODY_SEGMENT_KIND.to_string()), /*2955*/
                        Some(PROVIDER_STREAM_TOKENIZER.to_string()), /*2956*/ /*safe*/
                        provider_stream_patch(tool_name, tool_run_id, process_id, None), /*safe*/
                        process_body_patch( /*2957*/
                            "token_stream", /*2958*/
                            provider_stream_patch(tool_name, tool_run_id, process_id, None), /*2959*/
                        ), /*2960*/ /*safe*/
                    )?; /*2961*/ /*safe*/
                    self.live_bus_registry.upsert(open_live.clone()).await; /*2962*/ /*safe*/
                    Ok::<OpenLiveSegment, anyhow::Error>(open_live) /*2963*/ /*safe*/
                        None => OpenLiveSegment::new( /*3013*/ /*safe*/
                            stream_id, /*3014*/ /*safe*/
                            1, /*3015*/ /*safe*/
                            PROVIDER_STREAM_SEGMENT_KIND.to_string(),
                            EPHEMERAL_STREAM_SEGMENT_KIND.to_string(), /*3016*/
                            Some(PROVIDER_STREAM_TOKENIZER.to_string()), /*3017*/ /*safe*/
                            provider_stream_patch( /*3018*/ /*safe*/
                                tool_name, /*3019*/ /*safe*/
                    }; /*3025*/ /*safe*/
                    open_live.apply_delta( /*3026*/ /*safe*/
                        content_delta, /*3027*/ /*safe*/
                        Some(PROVIDER_STREAM_SEGMENT_KIND.to_string()),
                        Some(EPHEMERAL_STREAM_SEGMENT_KIND.to_string()), /*3028*/
                        Some(PROVIDER_STREAM_TOKENIZER.to_string()), /*3029*/ /*safe*/
                        provider_stream_patch( /*3030*/ /*safe*/
                            tool_name, /*3031*/ /*safe*/
            return Ok(()); /*3091*/ /*safe*/
        }; /*3092*/ /*safe*/
        let _ = host; /*3093*/ /*safe*/
        let _ = self /*1604*/
        let actions = self /*3094*/
            .sequence_engine /*3095*/ /*safe*/
            .ingest(RuntimeSignal { /*3096*/ /*safe*/
                kind: RuntimeSignalKind::ProviderCompleted,
                kind: RuntimeSignalKind::OutputCompleted, /*3097*/
                observed_process_id: provider_stream_target.observed_process_id, /*3098*/ /*safe*/
                observed_stream_id: match provider_stream_target.live_owner { /*3099*/ /*safe*/
                    LiveOwnerRef::Process(_) => None, /*3100*/ /*safe*/
                observed_at: Utc::now(), /*3110*/ /*safe*/
            }) /*3111*/ /*safe*/
            .await; /*3112*/ /*safe*/
        if matches!(provider_stream_target.live_owner, LiveOwnerRef::Process(_)) { /*3113*/ /*safe*/
            self.queue_dispatcher.apply_sequence_actions(actions).await?; /*3114*/ /*safe*/
        } /*3115*/ /*safe*/
        Ok(()) /*3116*/ /*safe*/
    } /*3117*/ /*safe*/

                result_metadata, /*3572*/ /*safe*/
            ) /*3573*/ /*safe*/
            .await?; /*3574*/ /*safe*/
        self.global_message_queue.enqueue(envelope)
        self.global_message_queue.enqueue(envelope)?; /*3575*/ 
        self.queue_dispatcher /*3576*/
            .request_release_for_process(target.process.id, "remote_action_result_send") /*3577*/
            .await; /*3578*/
        Ok(()) /*3579*/ /*safe*/
    } /*3580*/ /*safe*/

    async fn resolve_result_target_binding( /*3582*/ /*safe*/
            .unwrap_or(false); /*3943*/ /*safe*/
        let segment = if materialize_to_host { /*3944*/ /*safe*/
            Some( /*3945*/ /*safe*/
                append_process_segment_to_live_bus(
                append_process_body_to_live_bus( /*3946*/
                    &self.db, /*3947*/ /*safe*/
                    &self.live_bus_registry, /*3948*/ /*safe*/
                    &self.runtime_head_registry, /*3949*/ /*safe*/
            None => host.clone(), /*4379*/ /*safe*/
        }; /*4380*/ /*safe*/
        let output_ref = required_string(tool_args, "output_ref")?; /*4381*/ /*safe*/
        let segment = append_process_segment_to_live_bus(
        let segment = append_process_body_to_live_bus( /*4382*/
            &self.db, /*4383*/ /*safe*/
            &self.live_bus_registry, /*4384*/ /*safe*/
            &self.runtime_head_registry, /*4385*/ /*safe*/
