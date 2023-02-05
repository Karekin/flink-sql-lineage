package com.lineage.server.interfaces.controller;

import com.lineage.server.application.cqe.command.catalog.CreateCatalogCmd;
import com.lineage.server.application.cqe.command.catalog.UpdateCatalogCmd;
import com.lineage.server.application.dto.CatalogDTO;
import com.lineage.server.application.service.CatalogService;
import com.lineage.server.interfaces.result.Result;
import com.lineage.server.interfaces.result.ResultMessage;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;

/**
 * @description: CatalogController
 * @author: HamaWhite
 * @version: 1.0.0
 * @date: 2023/2/5 12:29 PM
 */
@RestController
@RequestMapping("/catalogs")
public class CatalogController {

    @Resource
    private CatalogService catalogService;


    @GetMapping("/{catalogId}")
    public Result<CatalogDTO> queryCatalog(@PathVariable("catalogId") final Long catalogId) {
        CatalogDTO catalogDTO = catalogService.queryCatalog(catalogId);
        return Result.success(ResultMessage.DETAIL_SUCCESS, catalogDTO);
    }

    @PostMapping("")
    public Result<Long> createCatalog(@RequestBody final CreateCatalogCmd createCatalogCmd) {
        Long catalogId = catalogService.createCatalog(createCatalogCmd);
        return Result.success(ResultMessage.CREATE_SUCCESS, catalogId);
    }

    @PutMapping("/{catalogId}")
    public Result<Boolean> updateCatalog(@PathVariable("catalogId") final Long catalogId,
                                      @RequestBody final UpdateCatalogCmd updateCatalogCmd) {
        updateCatalogCmd.setCatalogId(catalogId);
        Boolean result = catalogService.updateCatalog(updateCatalogCmd);
        return Boolean.TRUE.equals(result)
                ? Result.success(ResultMessage.UPDATE_SUCCESS)
                : Result.error(ResultMessage.UPDATE_FAILED);
    }

    @DeleteMapping("/{catalogId}")
    public Result<Boolean> deleteCatalog(@PathVariable("catalogId") final Long catalogId) {
        Boolean result = catalogService.deleteCatalog(catalogId);
        return Boolean.TRUE.equals(result)
                ? Result.success(ResultMessage.DELETE_SUCCESS)
                : Result.success(ResultMessage.DELETE_FAILED);
    }

}
